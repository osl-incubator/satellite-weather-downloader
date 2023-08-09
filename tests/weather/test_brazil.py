import unittest
from satellite.weather import brazil


class TestExtraction(unittest.TestCase):
    def test_extract_latlons_from_geocode(self):
        ex = brazil.extract_latlons.from_geocode
        self.assertEqual(ex(3304557), (-22.9129, -43.2003))
        self.assertEqual(ex(4200705), (-27.7001, -49.3273))
        self.assertEqual(ex(4306734), (-27.5103, -54.3577))
        self.assertEqual(ex(3153509), (-20.4208, -41.9670))
        self.assertEqual(ex(4218707), (-28.4713, -49.0144))
        self.assertEqual(ex(2801504), (-10.6449, -36.9887))
        self.assertEqual(ex(1703305), (-8.96306, -48.1650))

    def test_extract_coordinates_from_latlon(self):
        ex = brazil.extract_coordinates.from_latlon
        self.assertEqual(ex(-22.9129, -43.2003), (-22.75, -23.0, -43.0, -43.25))
        self.assertEqual(ex(-27.7001, -49.3273), (-27.5, -27.75, -49.25, -49.5))
        self.assertEqual(ex(-27.5103, -54.3577), (-27.5, -27.75, -54.25, -54.5))
        self.assertEqual(ex(-20.4208, -41.9670), (-20.25, -20.5, -41.75, -42.0))
        self.assertEqual(ex(-28.4713, -49.0144), (-28.25, -28.5, -49.0, -49.25))
        self.assertEqual(ex(-10.6449, -36.9887), (-10.5, -10.75, -36.75, -37.0))
        self.assertEqual(ex(-8.96306, -48.1650), (-8.75, -9.0, -48.0, -48.25))

if __name__ == "__main__":
    unittest.main()
