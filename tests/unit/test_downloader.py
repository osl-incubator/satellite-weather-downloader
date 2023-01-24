import unittest
import uuid
import shutil
from pathlib import Path
from unittest.mock import Mock
from satellite_downloader.utils.connection import connect

class TestDownloaderConnection(unittest.TestCase):
    uid = '123456'
    key = str(uuid.uuid4())

    cdsapirc_file = Path(Path.home() / '.cdsapirc')
    cdsapirc_bak = Path(Path.home() / '.cdsapirc.bak')
    
    @classmethod
    def setUpClass(cls):
        if cls.cdsapirc_file.exists():
            shutil.move(cls.cdsapirc_file, cls.cdsapirc_bak)

    @classmethod
    def tearDownClass(cls):
        if cls.cdsapirc_bak.exists():
            shutil.move(cls.cdsapirc_bak, cls.cdsapirc_file)

    def test_copernicus_credentials_connection(self):
        connect(self.uid, self.key)
        self.assertTrue(self.cdsapirc_file.exists())

        with open(self.cdsapirc_file, 'r') as f:
            url = f.readline()
            self.assertEqual(
                url,
                'url: https://cds.climate.copernicus.eu/api/v2\n'
            )

            key = f.readline()
            self.assertEqual(
                key,
                f'key: {self.uid}:{self.key}'
                )
        

if __name__ == '__main__':
    unittest.main()
