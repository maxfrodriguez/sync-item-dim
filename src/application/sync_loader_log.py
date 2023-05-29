
from src.infrastructure.repository.loader_log_impl import LoaderLogImpl


async def finish_synchronization(lowest_modlog: int, highest_modlog: int, fact_movements_loaded: int) -> None:
    loader_log_repository = LoaderLogImpl()
    await loader_log_repository.save_latest_loader_logs(
        lowest_modlog=lowest_modlog, highest_modlog=highest_modlog, fact_movements_loaded=fact_movements_loaded
    )