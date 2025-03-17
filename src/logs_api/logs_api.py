from typing import Any
from dataclasses import dataclass

from tapi_yandex_metrika import YandexMetrikaLogsapi


@dataclass
class OperationResult:
    success: bool
    error: Exception | None = None


class LogsAPI:

    def __init__(
        self,
        auth_token: str,
        counter_id: int,
        fields: list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        source: str | list[str] = ["visits", "hits"],
        attribution: str = "LASTSIGN",
        params: dict[str, Any] = {},
    ):
        self.client = YandexMetrikaLogsapi(
            access_token=auth_token, default_url_params={"counterId": counter_id}
        )

        self.params = {
            "source": source,
            "attribution": attribution,
        }

        if fields and len(fields) > 0:
            self.params.update({"fields": fields})
        if start_date:
            self.params.update({"date1": start_date})
        if end_date:
            self.params.update({"date2": end_date})

        self.params.update(params)

    def create_report(self, params: dict[str, Any] = {}) -> int:
        result = self.client.create().post(params=self.params | params)
        request_id = result["log_request"]["request_id"]

        return request_id

    def check_reporting_capability(
        self, params: dict[str, Any] = {}
    ) -> OperationResult:
        try:
            self.client.evaluate().get(params=self.params | params)
            return OperationResult(True)
        except Exception as e:
            return OperationResult(False, e)

    def get_report_info(self, request_id: int):
        return self.client.info(requestId=request_id).get()

    def get_all_reports_info(self):
        return self.client.allinfo().get()

    def is_report_ready(self, request_id: int) -> bool:
        info = self.get_report_info(request_id)
        return info["log_request"]["status"] == "processed"

    def download_report_part(self, request_id: int, part_num: int):
        return self.client.download(requestId=request_id, partNumber=part_num).get()

    def delete_report(self, request_id: int) -> OperationResult:
        try:
            self.client.clean(requestId=request_id).post()
            return OperationResult(True)
        except Exception as e:
            return OperationResult(False, e)
