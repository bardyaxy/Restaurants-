import types
from restaurants import gui


def test_make_gui(monkeypatch):
    created = {}

    class DummyTk:
        def __init__(self):
            created['root'] = True
        def title(self, text):
            created['title'] = text
        def mainloop(self):
            created['loop'] = True

    class DummyFrame:
        def __init__(self, master=None, **kw):
            created['frame'] = True
        def pack(self, *a, **kw):
            pass

    class DummyButton:
        def __init__(self, master=None, **kw):
            created.setdefault('widgets', []).append(kw.get('text'))
        def pack(self, *a, **kw):
            pass

    class DummyLabel:
        def __init__(self, master=None, **kw):
            created.setdefault('widgets', []).append(kw.get('text', 'label'))
        def pack(self, *a, **kw):
            pass

    class DummyEntry:
        def __init__(self, master=None, **kw):
            created.setdefault('widgets', []).append('entry')
        def pack(self, *a, **kw):
            pass
        def get(self):
            return ''

    class DummyPB:
        def __init__(self, master=None, **kw):
            created.setdefault('widgets', []).append('progress')
        def pack(self, *a, **kw):
            pass

    monkeypatch.setattr(gui.tk, 'Tk', DummyTk)
    monkeypatch.setattr(gui.tk, 'Frame', DummyFrame)
    monkeypatch.setattr(gui.tk, 'Button', DummyButton)
    monkeypatch.setattr(gui.tk, 'Label', DummyLabel)
    monkeypatch.setattr(gui.tk, 'Entry', DummyEntry)
    monkeypatch.setattr(gui.ttk, 'Progressbar', DummyPB)

    root = gui.make_gui()
    assert isinstance(root, DummyTk)
    assert created['title'] == 'Olympia Restaurants'
    assert created['widgets'] == [
        'ZIP codes (comma separated)',
        'entry',
        'Refresh Restaurants',
        'Fetch Toast Leads',
        'Collected: 0',
        'progress',
    ]


def test_run_refresh(monkeypatch):
    called = {}

    def fake_main(argv=None):
        called['refresh'] = argv
        gui.refresh_restaurants.smb_restaurants_data[:] = [1, 2, 3]

    monkeypatch.setattr(gui.refresh_restaurants, 'main', fake_main)
    monkeypatch.setattr(gui.messagebox, 'showinfo', lambda *a, **kw: called.setdefault('info', True))

    class DummyEntry:
        def get(self):
            return '98765'

    class DummyLabel:
        def config(self, **kw):
            called['label'] = kw

    class DummyPB:
        def start(self):
            called['start'] = True
        def stop(self):
            called['stop'] = True

    monkeypatch.setattr(gui, 'zip_entry', DummyEntry())
    monkeypatch.setattr(gui, 'progress_label', DummyLabel())
    monkeypatch.setattr(gui, 'progress_bar', DummyPB())

    gui.run_refresh()

    assert called['refresh'] == ['--zips', '98765']
    assert called['info'] is True
    assert called['label']['text'] == 'Collected: 3'
    assert called['start'] and called['stop']


def test_run_toast(monkeypatch):
    called = {}
    monkeypatch.setattr(gui.toast_leads, 'main', lambda: called.setdefault('toast', True))
    monkeypatch.setattr(gui.messagebox, 'showinfo', lambda *a, **kw: called.setdefault('info', True))
    gui.run_toast()
    assert called == {'toast': True, 'info': True}

