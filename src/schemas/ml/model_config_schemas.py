from pydantic import BaseModel


class ModelConfigSchema(BaseModel):
    model_name: str
    import_path: str
