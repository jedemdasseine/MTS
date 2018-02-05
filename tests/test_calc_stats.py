import datetime

import calc_stats


class TestCalculateNullCountFloat:
    def test_calculate_null_count_float_load_date(self):
        assert (calc_stats.calculate_null_count_float(5, None, 1, 2, 0.5, "JIJIJ", datetime.date(2018, 2, 1)) == 6)

    def test_calculate_null_count_float_id(self):
        assert (calc_stats.calculate_null_count_float(5, datetime.date(2018, 2, 1), None, 2, 0.5,
                                                      "JIJIJ", datetime.date(2018, 2, 1)) == 6)

    def test_calculate_null_count_int_value(self):
        assert (calc_stats.calculate_null_count_float(5, datetime.date(2018, 2, 1), 1, None, 0.5,
                                                      "JIJIJ", datetime.date(2018, 2, 1)) == 6)

    def test_calculate_null_count_float_value(self):
        assert (calc_stats.calculate_null_count_float(5, datetime.date(2018, 2, 1), 1, 2, None,
                                                      "JIJIJ", datetime.date(2018, 2, 1)) == 6)

    def test_calculate_null_count_float_char_value(self):
        assert (calc_stats.calculate_null_count_float(5, datetime.date(2018, 2, 1), 1, 2, 0.5,
                                                      None, datetime.date(2018, 2, 1)) == 6)

    def test_calculate_null_count_float_date_value(self):
        assert (calc_stats.calculate_null_count_float(5, datetime.date(2018, 2, 1), 1, 2, 0.5,
                                                      "JIJIJ", None) == 6)

    def test_calculate_no_null_count_float(self):
        assert (calc_stats.calculate_null_count_float(5, datetime.date(2018, 2, 1), 1, 2, 0.5,
                                                      "JIJIJ", datetime.date(2018, 2, 1)) == 5)


class TestCalculateZeroCountFloat:
    def test_calculate_zero_count_float_id(self):
        assert (calc_stats.calculate_zero_count_float(5, 0, 2, 0.5) == 6)

    def test_calculate_zero_count_float_int_value(self):
        assert (calc_stats.calculate_zero_count_float(5, 1, 0, 0.5) == 6)

    def test_calculate_zero_count_float_value(self):
        assert (calc_stats.calculate_zero_count_float(5, 1, 2, 0) == 6)

    def test_calculate_no_zero_count_float(self):
        assert (calc_stats.calculate_zero_count_float(5, 1, 2, 0.5) == 5)


class TestCalculateEqualIdInt:
    def test_calculate_equal_id_int(self):
        assert (calc_stats.calculate_equal_id_int(5, 5, 5) == 6)

    def test_calculate_no_equal_id_int(self):
        assert (calc_stats.calculate_equal_id_int(5, 5, 6) == 5)


class TestCalculateSumOfIntValues:
    def test_calculate_sum_of_int_values(self):
        assert (calc_stats.calculate_sum_of_int_values(5, 6) == 11)


class TestCalculateSumOfFloatValues:
    def test_calculate_sum_of_float_values(self):
        assert (calc_stats.calculate_sum_of_float_values(0.2, 0.5) == 0.7)


class TestCalculateCount:
    def test_calculate_count(self):
        assert (calc_stats.calculate_count(5) == 6)


class TestCalculateAverageInt:
    def test_calculate_average_int(self):
        assert (calc_stats.calculate_average_int(5, 10) == 2)


class TestCalculateAverageFloat:
    def test_calculate_average_float(self):
        assert (calc_stats.calculate_average_float(5, 0.1) == 0.02)
