"""Simple Tkinter GUI for common tasks.

Provides buttons to run ``refresh-restaurants`` and ``toast-leads``
without using the command line.  This module also exposes a more
featureful interface with a ZIP code entry field, a progress bar and
basic error reporting.
"""

from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from . import refresh_restaurants, toast_leads

# Widgets created by :func:`make_gui`.  They are populated at runtime and used
# by :func:`run_refresh` to read ZIP codes and update progress information.
zip_entry: tk.Entry | None = None
progress_label: tk.Label | None = None
progress_bar: ttk.Progressbar | None = None


def run_refresh() -> None:
    """Run :func:`refresh_restaurants.main` and report completion.

    The ZIP codes are read from :data:`zip_entry` if available. Progress is
    indicated by an indeterminate progress bar and a label displaying the final
    number of collected entries. Any exception raised by the refresh process is
    shown in an error dialog.
    """
    global zip_entry, progress_label, progress_bar

    argv: list[str] = []
    if zip_entry is not None:
        zips = zip_entry.get().strip()
        if zips:
            argv = ["--zips", zips]

    if progress_label is not None:
        progress_label.config(text="Running…")
    if progress_bar is not None:
        progress_bar.start()

    try:
        refresh_restaurants.main(argv)
        count = len(refresh_restaurants.smb_restaurants_data)
        if progress_label is not None:
            progress_label.config(text=f"Collected: {count}")
        messagebox.showinfo("Refresh Complete", f"Collected {count} entries.")
    except Exception as exc:  # pragma: no cover - GUI feedback only
        if progress_label is not None:
            progress_label.config(text="Error")
        messagebox.showerror("Error", str(exc))
    finally:
        if progress_bar is not None:
            progress_bar.stop()


def run_toast() -> None:
    """Run :func:`toast_leads.main` and report completion."""
    try:
        toast_leads.main()
        messagebox.showinfo("Toast Complete", "Toast leads fetched.")
    except Exception as exc:  # pragma: no cover - GUI feedback only
        messagebox.showerror("Error", str(exc))


def make_gui() -> tk.Tk:
    """Create the GUI window and return the ``Tk`` instance."""
    global zip_entry, progress_label, progress_bar

    root = tk.Tk()
    root.title("Olympia Restaurants")

    frame = tk.Frame(root, padx=20, pady=20)
    frame.pack()

    tk.Label(frame, text="ZIP codes (comma separated)").pack(fill="x")
    zip_entry = tk.Entry(frame)
    zip_entry.pack(fill="x")

    btn_refresh = tk.Button(frame, text="Refresh Restaurants", command=run_refresh)
    btn_refresh.pack(fill="x", pady=(10, 0))

    btn_toast = tk.Button(frame, text="Fetch Toast Leads", command=run_toast)
    btn_toast.pack(fill="x")

    progress_label = tk.Label(frame, text="Collected: 0")
    progress_label.pack(fill="x", pady=(10, 0))

    progress_bar = ttk.Progressbar(frame, mode="indeterminate")
    progress_bar.pack(fill="x")

    return root


def main() -> None:
    """Launch the GUI application."""
    root = make_gui()
    root.mainloop()


if __name__ == "__main__":  # pragma: no cover - manual invocation
    main()
