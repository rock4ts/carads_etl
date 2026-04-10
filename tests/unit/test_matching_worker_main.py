from app.services.matching_service.main import _iter_sites


def test_iter_sites_filters_requested_subset() -> None:
    sites = ["avito", "drom", "auto"]

    selected = _iter_sites(sites, [" auto ", "missing"])

    assert selected == ["auto"]
