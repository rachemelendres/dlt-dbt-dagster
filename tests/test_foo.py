from dlt_dbt_dagster.foo import foo


def test_foo():
    assert foo("foo") == "foo"
