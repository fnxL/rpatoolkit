import pytest
from rpatoolkit.df.read_excel import _strip_punctuation


class TestStripPunctuation:
    @pytest.mark.parametrize(
        "input_text,replacement,expected",
        [
            ("First Name!", "", "First Name"),
            ("Last, Name", "", "Last Name"),
            ("Age?", "", "Age"),
            ("Email@domain.com", "", "Emaildomaincom"),
            ("Price: $100", "", "Price 100"),
            ("What's up?", "", "Whats up"),
            ("Ship. date", "", "Ship date"),
            ("100%", "", "100"),
        ],
    )
    def test_punctuation_with_default_replacement(
        self, input_text, replacement, expected
    ):
        assert _strip_punctuation(input_text, replacement) == expected

    @pytest.mark.parametrize(
        "input_text,replacement,expected",
        [
            ("First Name!", "_", "First Name_"),
            ("Last, Name", "_", "Last_ Name"),
            ("Age?", "_", "Age_"),
            ("Email@domain.com", "_", "Email_domain_com"),
            ("Price: $100", "_", "Price_ _100"),
        ],
    )
    def test_punctuation_with_custom_replacement(
        self, input_text, replacement, expected
    ):
        assert _strip_punctuation(input_text, replacement) == expected

    def test_no_punctuation(self):
        result = _strip_punctuation("Hello World")
        assert result == "Hello World"

    def test_only_punctuation(self):
        result = _strip_punctuation("!@#$%^&*()+-=[]{}|;':\",./<>?")
        assert result == ""

    def test_mixed_alphanumeric(self):
        result = _strip_punctuation("Test123!@#ABC")
        assert result == "Test123ABC"

    def test_whitespace_preservation(self):
        result = _strip_punctuation("Hello,   World!")
        assert result == "Hello   World"

    def test_leading_trailing_spaces(self):
        result = _strip_punctuation("  Hello, World!  ")
        assert result == "Hello World"

    def test_unicode_characters(self):
        result = _strip_punctuation("Café résumé naïve")
        assert result == "Café résumé naïve"

    def test_empty_string(self):
        result = _strip_punctuation("")
        assert result == ""

    def test_non_string_input_type_error(self):
        with pytest.raises(TypeError, match="Input must be a string"):
            _strip_punctuation(None)

        with pytest.raises(TypeError, match="Input must be a string"):
            _strip_punctuation(123)

        with pytest.raises(TypeError, match="Input must be a string"):
            _strip_punctuation([])

        with pytest.raises(TypeError, match="Input must be a string"):
            _strip_punctuation({})

    def test_underscores_unchanged(self):
        result = _strip_punctuation("hello_world_test")
        assert result == "hello_world_test"

    def test_hyphens_with_replacement(self):
        result = _strip_punctuation("hello-world-test", "_")
        assert result == "hello_world_test"

    def test_complex_string(self):
        """Test a complex string with various characters."""
        input_str = "Hello, World! This is a test: 123-456.789@domain.com"
        expected = "Hello World This is a test 123456789domaincom"
        result = _strip_punctuation(input_str)
        assert result == expected
