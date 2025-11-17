import csv
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    MetadataChangeProposalWrapper,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    EditablePropertiesClass
)

DATAHUB_GMS_URL = "http://your-gms-host:8080"
emitter = DatahubRestEmitter(DATAHUB_GMS_URL)

with open('update_desc.csv', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        urn = row['urn']
        desc = row['description']

        # 테이블 설명 업데이트: EditableProperties
        if '.column:' not in urn:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=EditablePropertiesClass(description=desc)
            )
            emitter.emit(mcp)
        # 컬럼 설명 업데이트: EditableSchemaMetadata (단일 컬럼)
        else:
            field_path = urn.split('.column:')[1]
            dataset_urn = urn.split('.column:')[0]
            field_info = EditableSchemaFieldInfoClass(
                fieldPath=field_path,
                description=desc
            )
            editable_aspect = EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[field_info]
            )
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=editable_aspect
            )
            emitter.emit(mcp)
