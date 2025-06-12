"""Simple Tkinter GUI for common tasks.

Provides buttons to run ``refresh-restaurants`` and ``toast-leads``
without using the command line.
"""

from __future__ import annotations

import tkinter as tk
from tkinter import messagebox

try:
    from . import refresh_restaurants, toast_leads
except ImportError:  # pragma: no cover - fallback for running as script
    import refresh_restaurants  # type: ignore
    import toast_leads  # type: ignore


def run_refresh() -> None:
    """Run :func:`refresh_restaurants.main` and report completion."""
    try:
        refresh_restaurants.main([])
        messagebox.showinfo("Refresh Complete", "Restaurant data refreshed.")
    except Exception as exc:  # pragma: no cover - GUI feedback only
        messagebox.showerror("Error", str(exc))


def run_toast() -> None:
    """Run :func:`toast_leads.main` and report completion."""
    try:
        toast_leads.main()
        messagebox.showinfo("Toast Complete", "Toast leads fetched.")
    except Exception as exc:  # pragma: no cover - GUI feedback only
        messagebox.showerror("Error", str(exc))


def make_gui() -> tk.Tk:
    """Create the GUI window and return the ``Tk`` instance."""
    root = tk.Tk()
    root.title("Olympia Restaurants")

    frame = tk.Frame(root, padx=20, pady=20)
    frame.pack()

    btn_refresh = tk.Button(frame, text="Refresh Restaurants", command=run_refresh)
    btn_refresh.pack(fill="x")

    btn_toast = tk.Button(frame, text="Fetch Toast Leads", command=run_toast)
    btn_toast.pack(fill="x", pady=(10, 0))

    return root


def main() -> None:
    """Launch the GUI application."""
    root = make_gui()
    root.mainloop()


if __name__ == "__main__":  # pragma: no cover - manual invocation
    main()
