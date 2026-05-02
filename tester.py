def very_bad_function(a, b):
    x = 1 + 2
    y = "this is a very very very very very very very very very very very very very very very very very very very very very long string that should definitely exceed one hundred and twenty characters in length"
    return {"result": a + b, "extra": x}


def another_function():
    data = {"a": 1, "b": 2, "c": 3}
    for k, v in data.items():
        print(k, v)


class TestClass:
    def __init__(self, value):
        self.value = value

    def compute(self):
        result = self.value * 2 + 5
        unused_variable = 123
        return result


def long_call():
    return (
        very_bad_function(
            1,
            2,
        )
        if True
        else very_bad_function(
            3,
            4,
        )
    )
