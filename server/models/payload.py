from enum import Enum
from bson import ObjectId
from pydantic_core import core_schema


class LLMEnum(str, Enum):
    chatgpt = "chatgpt"
    deepseek = "deepseek"
    grok = "grok"
    claude = "claude"
    llama = "llama"
    ollama_mistral = "ollama-mistral"
    ollama_tiny_llama = "ollama-tiny-llama"


class PyObjectId(ObjectId):
    """
    Custom type for MongoDB ObjectId,
    inheriting from bson.ObjectId to maintain ObjectId functionality.
    """

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type, handler
    ) -> core_schema.CoreSchema:
        """
        Defines the Pydantic CoreSchema for PyObjectId.
        This ensures that any valid input (string or ObjectId)
        is converted into a PyObjectId instance for both validation and serialization.
        """
        # Define the schema for how Pydantic should validate/parse inputs
        validation_schema = core_schema.union_schema(
            [
                core_schema.is_instance_schema(
                    ObjectId
                ),  # Accept existing ObjectId instances
                core_schema.str_schema(),  # Accept string representation
            ]
        )

        # CORRECTED LINE: Changed to field_after_validator_function
        pyobjectid_schema = core_schema.with_info_after_validator_function(
            cls.validate_pyobjectid_input,  # Our dedicated input validator
            validation_schema,
        )

        # Define the serialization logic: how PyObjectId instances are converted to strings
        serialization_schema = core_schema.plain_serializer_function_ser_schema(
            cls._serialize_pyobjectid_to_str,
            return_schema=core_schema.str_schema(),  # Explicitly state it serializes to a string
            when_used="always",  # Ensure it's always used for serialization
        )

        # Combine validation and serialization
        return core_schema.json_or_python_schema(
            json_schema=core_schema.str_schema(
                min_length=24,
                max_length=24,
                pattern=r"^[0-9a-fA-F]{24}$",
            ),
            python_schema=pyobjectid_schema,
            serialization=serialization_schema,
            metadata={"description": "MongoDB ObjectId string"},
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: core_schema.CoreSchema, handler
    ) -> core_schema.JsonSchema:
        """
        Provides the JSON Schema specific details for OpenAPI documentation.
        This is separate from the core_schema for validation/serialization.
        """
        return {
            "type": "string",
            "minLength": 24,
            "maxLength": 24,
            "pattern": r"^[0-9a-fA-F]{24}$",
            "example": "507f1f77bcf86cd799439011",
            "format": "ObjectId",
        }

    # --- Validation Logic ---
    @classmethod
    def validate_pyobjectid_input(
        cls, v, info
    ):  # This method handles the actual validation logic for inputs
        if isinstance(v, ObjectId):
            return cls(v)
        if isinstance(v, str):
            if ObjectId.is_valid(v):
                return cls(v)
            raise ValueError("Invalid ObjectId string")
        raise TypeError("ObjectId must be a string or ObjectId instance")

    # --- Serialization Logic ---
    @classmethod
    def _serialize_pyobjectid_to_str(
        cls, instance: "PyObjectId | ObjectId", info="something"
    ) -> str:
        """
        Converts a PyObjectId or raw ObjectId instance to its string representation for serialization.
        """
        if not isinstance(instance, (PyObjectId, ObjectId)):
            raise TypeError(
                f"Unexpected type for serialization: {type(instance)}. Expected PyObjectId or ObjectId."
            )
        return str(instance)

    def __repr__(self):
        return f"PyObjectId('{super().__str__()}')"

    def __str__(self):
        return super().__str__()