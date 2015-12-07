import unittest
import My_RIPE
from My_RIPE import Measurement, Measurement_Data, Ping_Data
import json, os, sys

TEST_ARG = None

class TestSummaries(unittest.TestCase):

  def test_summaries(self):
    print("--test_summaries--")
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    summaries_file = "test_summaries_file.txt"
    measurement = Measurement(1001, summaries_file)
    summary, local_hit = measurement._get_summary(summaries_file)

    self.assertEqual(summary, measurement.summary)
    self.assertTrue(local_hit)

    local_dict = measurement._get_dict_from_nonempty_file(summaries_file)
    local_summary = local_dict["1001"]

    self.assertEqual(local_summary, measurement.summary)


class MeasTestCase(unittest.TestCase):
  USE_LOCAL_SUMMARIES = False
  TEST_DATA_DOWNLOAD = True

  def setUp(self):
    print("--setUp--")
    self.measurement_ids = [1001, 10116, 5004]

    if not self.USE_LOCAL_SUMMARIES:
      # create blank summaries_file
      self.fname = "summaries_file.txt"
      f = open(self.fname, "w")  
      f.close()
    else:
      self.fname = "summaries_file_local.txt"

    # instantiate measurements
    self.measurements = []
    self._add_measurements()

  def _add_measurements(self):
    for this_id in self.measurement_ids:
      self.measurements.append(Measurement(this_id, self.fname))


class TestMethods(MeasTestCase):

  def _make_dir_if_nonexists(self, dir_name):
    dir_path = os.path.join(os.getcwd(),dir_name)
    if not os.path.exists(dir_path):
      os.makedirs(dir_path)


  def test0(self):
    '''
    start with empty summaries_file, instantiate a measurement, check that summary got added to file
    '''
    print("--test0--")
    i = 0
    summaries_dict = self.measurements[i]._get_dict_from_nonempty_file(self.fname)
    
    self.assertEqual(summaries_dict[str(self.measurement_ids[i])]["msm_id"], self.measurement_ids[i])
    self.assertEqual(self.measurements[i].local_hit, self.USE_LOCAL_SUMMARIES)
    self.assertEqual(self.measurements[i].type, "ping")

    measurement2 = Measurement(self.measurement_ids[i], self.fname)
    self.assertTrue(measurement2.local_hit)

    self._make_dir_if_nonexists("data")
    curr_path = os.path.dirname(os.path.realpath(__file__))
    data_path = os.path.join(curr_path, "data")
    os.chdir(data_path)
    file1 = measurement2._fetch_to_local(1446336000, 1446336100)
    file2 = measurement2._fetch_to_local(1446422400, 1446422500)
    list_of_entries = os.listdir(data_path)
    list_of_files = [f for f in list_of_entries if os.path.isfile(os.path.join(data_path, f))]
    self.assertEqual(len(list_of_files), 2)

    os.chdir(curr_path)
    ping_data = Ping_Data(measurement2.measurement_id, file1, self.fname)
    self.assertTrue(ping_data.local_hit)

    list_of_headings = Ping_Data.get_ping_headings()
    csv_file = "test.csv"
    gold_file = "gold.csv"
    ping_data.prep_csv_file(csv_file, list_of_headings)
    ping_data.write_data_to_csv(csv_file, probe_id = 10008)

    with open(csv_file) as csv_file_obj:
      with open(gold_file) as gold_file_obj:
        self.assertMultiLineEqual(csv_file_obj.read(), gold_file_obj.read())

    i = 1
    summaries_dict = self.measurements[i]._get_dict_from_nonempty_file(self.fname)
    
    self.assertEqual(summaries_dict[str(self.measurement_ids[i])]["msm_id"], self.measurement_ids[i])
    self.assertEqual(self.measurements[i].local_hit, self.USE_LOCAL_SUMMARIES)
    self.assertEqual(self.measurements[i].type, "dns")

    measurement2 = Measurement(self.measurement_ids[i], self.fname)
    self.assertTrue(measurement2.local_hit)

    i = 2
    summaries_dict = self.measurements[i]._get_dict_from_nonempty_file(self.fname)
    
    self.assertEqual(summaries_dict[str(self.measurement_ids[i])]["msm_id"], self.measurement_ids[i])
    self.assertEqual(self.measurements[i].local_hit, self.USE_LOCAL_SUMMARIES)
    self.assertEqual(self.measurements[i].type, "traceroute")

    measurement2 = Measurement(self.measurement_ids[i], self.fname)
    self.assertTrue(measurement2.local_hit)

  # def test2(self):
  #   '''
  #   start with empty summaries_file, instantiate a measurement, instantiate another measurement with same id.
  #   check that we're getting the summary locally, not from internet
  #   '''
    


  # def test_split(self):
  #     s = 'hello world'
  #     self.assertEqual(s.split(), ['hello', 'world'])
  #     # check that s.split fails when the separator is not a string
  #     with self.assertRaises(TypeError):
  #         s.split(2)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        passed_arg = sys.argv.pop()

        if passed_arg == '1':
          MeasTestCase.USE_LOCAL_SUMMARIES = True
    unittest.main()