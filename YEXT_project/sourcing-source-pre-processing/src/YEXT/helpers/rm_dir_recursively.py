from pathlib import Path


def rm_dir_recursively(pth):
    """
    Remove directory recursively
    """
    pth = Path(pth)
    for child in pth.glob("*"):
        if child.is_file():
            child.unlink()
        else:
            rm_dir_recursively(child)
    pth.rmdir()
