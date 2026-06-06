"""Prefect deployment configuration for scraper flow."""
from __future__ import annotations

from prefect_dags.flows.scraper_flow import scraper_flow


def create_scraper_deployment(
    name: str = "scraper-deployment",
):
    """
    Create a deployment for the scraper flow (no schedule).

    Args:
        name: Deployment name

    Returns:
        Serve function (blocks)
    """
    return scraper_flow.serve(
        name=name,
        description="Regulatory circular scraper",
    )


if __name__ == "__main__":
    create_scraper_deployment()
