import satellite_downloader as downloader

def get_version() -> str:
    try:
        return importlib_metadata.version(__name__)
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return '1.4.0'  # changed by semantic-release


version: str = get_version()
__version__: str = version
