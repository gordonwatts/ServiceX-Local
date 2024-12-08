class ScienceWSL2:
    def __init__(self, wsl2_container: str, atlas_release: str):
        """Science image will run in a WSL2 container with the specified ATLAS release

        Args:
            wsl2_container (str): Which WSL2 container should be used ("al9_atlas")
            atlas_release (str): Which release should be used ("22.2.107")
        """
        self._release = atlas_release
        self._container = wsl2_container
