import io
import queue
import threading
import unittest
from collections import deque
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import Mock

from tapo_cloud_app import CloudMediaWorker, Handler, NATIVE_STREAM_RESET, safe_media_error


class MediaTests(unittest.TestCase):
    def request_stream(self, chunks, error=''):
        subscriber = queue.Queue()
        for chunk in chunks:
            subscriber.put(chunk)
        worker = SimpleNamespace(
            subscribe_native_video=lambda: subscriber,
            unsubscribe_native_video=Mock(),
            last_error=error, media_stage='relay connection', ffmpeg_errors=deque(),
        )
        state = SimpleNamespace(worker=lambda index: worker, devices=[{}])
        class TestHandler(Handler):
            context = SimpleNamespace(current_state=lambda: state)
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), TestHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connection = HTTPConnection('127.0.0.1', server.server_port, timeout=4)
            connection.request('GET', '/stream/0.mp4')
            response = connection.getresponse()
            result = response.status, response.read().decode()
            connection.close()
            worker.unsubscribe_native_video.assert_called_once_with(subscriber)
            return result
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_reset_before_headers_returns_reason_not_empty_connection(self):
        status, body = self.request_stream([NATIVE_STREAM_RESET], 'relay video is encrypted: decoder is not available yet')
        self.assertEqual(status, 503)
        self.assertIn('encrypted', body)

    def test_reset_without_reason_is_still_http_error(self):
        status, body = self.request_stream([NATIVE_STREAM_RESET])
        self.assertEqual(status, 503)
        self.assertIn('relay restarted', body)

    def test_failure_without_reset_is_reported_during_startup(self):
        status, body = self.request_stream([], 'FFmpeg executable missing')
        self.assertEqual(status, 503)
        self.assertIn('FFmpeg executable missing', body)

    def test_reset_after_headers_does_not_append_second_http_response(self):
        status, body = self.request_stream([b'video-fragment', NATIVE_STREAM_RESET])
        self.assertEqual((status, body), (200, 'video-fragment'))

    def test_second_subscriber_does_not_interrupt_startup_again(self):
        worker = CloudMediaWorker.__new__(CloudMediaWorker)
        worker.lock = threading.Lock()
        worker.native_video_subscribers = set()
        worker.native_video_running = False
        worker.native_init_segment = b''
        worker.wake = threading.Event()
        worker._interrupt_relay = Mock()
        worker.last_error = 'previous failure'
        first = worker.subscribe_native_video()
        worker.subscribe_native_video()
        worker._interrupt_relay.assert_called_once()
        self.assertEqual(worker.last_error, 'previous failure')
        self.assertIn(first, worker.native_video_subscribers)

    def test_ffmpeg_stderr_is_drained_and_bounded(self):
        worker = CloudMediaWorker.__new__(CloudMediaWorker)
        worker.ffmpeg_errors = deque(maxlen=8)
        process = SimpleNamespace(stderr=io.BytesIO(b'bad codec\n' * 20))
        worker._read_ffmpeg_errors(process)
        self.assertEqual(len(worker.ffmpeg_errors), 8)
        self.assertEqual(worker.ffmpeg_errors[-1], 'bad codec')
        self.assertTrue(process.stderr.closed)

    def test_diagnostic_redacts_relay_url_and_token(self):
        result = safe_media_error('failure https://relay.example/path?token=secret X-token=private')
        self.assertNotIn('secret', result)
        self.assertNotIn('private', result)
        self.assertNotIn('relay.example', result)


if __name__ == '__main__':
    unittest.main()
