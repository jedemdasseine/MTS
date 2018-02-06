import datetime

from Project import generate

dates = [datetime.date(2018, 2, 5), datetime.date(2018, 2, 4), datetime.date(2018, 2, 3), datetime.date(2018, 2, 2),
         datetime.date(2018, 2, 1)]


class TestGenerateDatelist:

    def test_generate_datelist(self):
        assert (generate.generate_datelist(datetime.date(2018, 2, 5)) == dates)


class TestGenerateLoadDate:

    def test_generate_load_date_none_value(self):
        assert (generate.generate_load_date(dates, len(dates)) is None)

    def test_generate_load_date_positive(self):
        assert (generate.generate_load_date(dates, 3) == datetime.date(2018, 2, 2))


class TestGenerateID:

    def test_generate_id_none_value(self):
        assert (generate.generate_id(50) is None)

    def test_generate_id_positive_id(self):
        assert (generate.generate_id(500) == 500)


class TestGenerateIntValue:

    def test_generate_int_value_zero_value(self):
        assert (generate.generate_int_value(196, 0) == 0)

    def test_generate_int_value_none_value(self):
        assert (generate.generate_int_value(196, 80) is None)

    def test_generate_int_value_equal_id(self):
        assert (generate.generate_int_value(196, 157) == 196)

    def test_generate_int_value_equal_iter(self):
        assert (generate.generate_int_value(196, 255) == 255)


class TestGenerateCharValue:

    def test_generate_char_none_value(self):
        assert (generate.generate_char_value(-1) is None)

    def test_generate_char(self):
        assert (len(generate.generate_char_value(5)) == 5)

    def test_generate_char_type(self):
        assert isinstance(generate.generate_char_value(5), str)


class TestGenerateDateValue:

    def test_generate_date_value_none_value(self):
        assert (generate.generate_date_value(dates, len(dates)) is None)

    def test_generate_date_value(self):
        assert (generate.generate_date_value(dates, 3) == datetime.date(2018, 2, 2))


class TestRandomiseIntNumber:

    def test_generate_random_int_number_value(self):
        assert (generate.randomize_int_number(1, 10000) in range(1, 10000))


class TestRandomiseFloatNumber:

    def test_generate_random_float_number_type(self):
        assert isinstance(generate.randomize_float_number(), float)
