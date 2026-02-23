from bson import ObjectId

class Helper:
    def _is_object_id(value: str) -> bool:
        try:
            ObjectId(value)
            return True
        except Exception:
            return False


    def _is_int_id(value: str) -> bool:
        return value.isdigit()