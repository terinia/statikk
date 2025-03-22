from typing import TypeVar, get_origin, Union, get_args

T = TypeVar("T", bound="DatabaseModel")


def inspect_optional_field(model_class, field_name):
    field_type = model_class.model_fields[field_name].annotation

    is_optional = False
    inner_type = field_type

    if get_origin(field_type) is Union:
        args = get_args(field_type)
        if len(args) == 2 and args[1] is type(None):
            is_optional = True
            inner_type = args[0]

    elif hasattr(field_type, "__origin__") and field_type.__origin__ is Union:
        args = getattr(field_type, "__args__", [])
        if len(args) == 2 and args[1] is type(None):
            is_optional = True
            inner_type = args[0]

    return (is_optional, inner_type)
