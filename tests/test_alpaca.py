import sys
from pathlib import Path
from types import ModuleType
import importlib

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def test_get_session_uses_env(monkeypatch):
    calls = {}

    class DummySession:
        def __init__(self):
            calls['created'] = True
            self.headers = {}

    dummy_requests = ModuleType('requests')
    dummy_requests.Session = DummySession

    with monkeypatch.context() as m:
        m.setitem(sys.modules, 'requests', dummy_requests)
        m.setenv('ALPACA_API_KEY', 'key')
        m.setenv('ALPACA_SECRET_KEY', 'secret')
        alpaca = importlib.import_module('bots.alpaca')
        sess = alpaca.get_session()

    assert calls.get('created')
    assert sess.headers['APCA-API-KEY-ID'] == 'key'
    assert sess.headers['APCA-API-SECRET-KEY'] == 'secret'
    assert hasattr(sess, 'base_url')
