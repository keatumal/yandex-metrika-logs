from typing import Any
from dataclasses import dataclass

from tapi_yandex_metrika import YandexMetrikaLogsapi

@dataclass
class ReportCheckResult:
    success: bool
    error: Exception | None = None

class LogsAPI():

    def __init__(
            self,
            fields: list[str],
            auth_token: str,
            counter_id: int,
            start_date: str,
            end_date: str,
            source: str | list[str] = ['visits', 'hits'],
            params: dict[str, Any] = {}
    ):
        self.client = YandexMetrikaLogsapi(
            access_token=auth_token,
            default_url_params={'counterId': counter_id}
        )
        self.params = {
            'source': source,
            'fields': fields,
            'date1': start_date,
            'date2': end_date
        } | params

    def create_report(self, params: dict[str, Any] = {}) -> int:
        result = self.client.create().post(params=self.params | params)
        request_id = result["log_request"]["request_id"]

        return request_id

    def check_reporting_capability(self, params: dict[str, Any] = {}) -> tuple[bool, Exception | None]:
        try:
            self.client.evaluate().get(params=self.params | params)
            return ReportCheckResult(True)
        except Exception as e:
            return ReportCheckResult(False, e)

    def get_report_info(self, request_id: int) -> dict:
        return self.client.info(requestId=request_id).get()

    def is_report_ready(self, request_id: int) -> bool:
        info = self.get_report_info(request_id)
        return info["log_request"]["status"] == "processed"
    
    def download_report_part(self, request_id: int, part_num: int):
        return self.client.download(requestId=request_id, partNumber=part_num).get()