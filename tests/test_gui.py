import types
from restaurants import gui


def test_make_gui(monkeypatch):
    created = {}

    class DummyTk:
        def __init__(self):
            created["root"] = True

        def title(self, text):
            created["title"] = text

        def mainloop(self):
            created["loop"] = True

    class DummyFrame:
        def __init__(self, master=None, **kw):
            created["frame"] = True

        def pack(self, *a, **kw):
            pass

    class DummyButton:
        def __init__(self, master=None, **kw):
            created.setdefault("widgets", []).append(kw.get("text"))

        def pack(self, *a, **kw):
            pass

    monkeypatch.setattr(gui.tk, "Tk", DummyTk)
    monkeypatch.setattr(gui.tk, "Frame", DummyFrame)
    monkeypatch.setattr(gui.tk, "Button", DummyButton)

    root = gui.make_gui()
    assert isinstance(root, DummyTk)
    assert created["title"] == "Olympia Restaurants"
    assert created["widgets"] == ["Refresh Restaurants", "Fetch Toast Leads"]


def test_run_refresh(monkeypatch):
    called = {}
    monkeypatch.setattr(
        gui.refresh_restaurants,
        "main",
        lambda argv=None: called.setdefault("refresh", True),
    )
    monkeypatch.setattr(
        gui.messagebox, "showinfo", lambda *a, **kw: called.setdefault("info", True)
    )
    gui.run_refresh()
    assert called == {"refresh": True, "info": True}


def test_run_toast(monkeypatch):
    called = {}
    monkeypatch.setattr(
        gui.toast_leads, "main", lambda: called.setdefault("toast", True)
    )
    monkeypatch.setattr(
        gui.messagebox, "showinfo", lambda *a, **kw: called.setdefault("info", True)
    )
    gui.run_toast()
    assert called == {"toast": True, "info": True}
