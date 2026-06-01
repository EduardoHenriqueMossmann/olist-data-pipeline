import unittest
import os
import pandas as pd
from datetime import datetime

class TestOlistPipeline(unittest.TestCase):
    
    def setUp(self):
        """Runs before every test. Sets up paths to check."""
        self.raw_data_dir = "C:/Users/eduar/OneDrive/Documentos/data_science/olist/"
        self.orders_file = os.path.join(self.raw_data_dir, "olist_orders_dataset.csv")

    def test_source_files_exist(self):
        """Ensure the core master dataset is present before running ETL."""
        self.assertTrue(os.path.exists(self.orders_file), "Master orders file is missing!")

    def test_date_ordering(self):
        """Test business logic: Ensure end dates are strictly after start dates."""
        start = "2017-01-01 00:00:00"
        end = "2017-03-31 23:59:59"
        
        dt_start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        dt_end = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
        
        self.assertGreater(dt_end, dt_start, "Pipeline Error: End date must be after start date.")

    def test_decimal_normalization_logic(self):
        """Simulate our comma-to-dot fixing logic to ensure it outputs a valid float."""
        raw_comma_value = "2,103"
        # This simulates what our pipeline/SQL script handles
        fixed_value = float(raw_comma_value.replace(",", "."))
        
        self.assertEqual(fixed_value, 2.103)
        self.assertLess(fixed_value, 3.0)

if __name__ == "__main__":
    unittest.main()