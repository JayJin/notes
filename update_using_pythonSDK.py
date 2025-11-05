from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn, make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    GlobalTagsClass, TagAssociationClass,
    GlossaryTermsClass, GlossaryTermAssociationClass,
    EditableSchemaFieldInfoClass, EditableSchemaMetadataClass
)
import csv

# Emitter 설정
emitter = DatahubRestEmitter("http://localhost:8080")

def update_metadata_from_csv(csv_file):
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            dataset_urn = row['resource']
            subresource = row.get('subresource', '')
            
            # 태그 업데이트
            if row.get('tags'):
                tags = [make_tag_urn(tag.strip()) 
                       for tag in row['tags'].split('|')]
                
                if subresource:
                    # 컬럼 레벨 태그
                    field_info = EditableSchemaFieldInfoClass(
                        fieldPath=subresource,
                        globalTags=GlobalTagsClass(
                            tags=[TagAssociationClass(tag=tag) for tag in tags]
                        )
                    )
                    
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=EditableSchemaMetadataClass(
                            editableSchemaFieldInfo=[field_info]
                        )
                    )
                else:
                    # 데이터셋 레벨 태그
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=GlobalTagsClass(
                            tags=[TagAssociationClass(tag=tag) for tag in tags]
                        )
                    )
                
                emitter.emit(mcp)
            
            # Glossary Terms 업데이트
            if row.get('glossary_terms'):
                terms = [make_term_urn(term.strip()) 
                        for term in row['glossary_terms'].split('|')]
                
                if subresource:
                    # 컬럼 레벨 용어
                    field_info = EditableSchemaFieldInfoClass(
                        fieldPath=subresource,
                        glossaryTerms=GlossaryTermsClass(
                            terms=[GlossaryTermAssociationClass(urn=term) 
                                  for term in terms]
                        )
                    )
                    
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=EditableSchemaMetadataClass(
                            editableSchemaFieldInfo=[field_info]
                        )
                    )
                else:
                    # 데이터셋 레벨 용어
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=GlossaryTermsClass(
                            terms=[GlossaryTermAssociationClass(urn=term) 
                                  for term in terms]
                        )
                    )
                
                emitter.emit(mcp)

# 실행
update_metadata_from_csv('datahub_metadata_update.csv')
