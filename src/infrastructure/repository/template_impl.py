from datetime import datetime
from typing import List
from src.domain.entities.shipment import Shipment
from src.domain.repository.template_abc import TemplateABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.cross_cutting.shipment_helper import get_template_id
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SATemplate
from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector
from src.infrastructure.data_access.sybase.sql_anywhere_impl import Record

class TemplateImpl(TemplateABC):

    async def save_templates(self, templates: List[Record]):
        template_sa_list: List[SATemplate] = []
        for template in templates:
            new_sa_template = SATemplate(**template)
            new_sa_template.template_id = get_template_id(value=new_sa_template.template_id)
            new_sa_template.created_at = datetime.utcnow().replace(second=0, microsecond=0)
            template_sa_list.append(new_sa_template)

        async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
            wh_client.bulk_copy(template_sa_list)