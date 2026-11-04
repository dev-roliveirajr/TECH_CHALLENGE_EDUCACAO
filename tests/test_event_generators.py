import pytest

from generators.generate_alunos_events import generate_single_aluno_event
from generators.generate_metas_events import generate_single_meta_event


def test_student_event_uses_official_network_codes():
    events = [generate_single_aluno_event() for _ in range(50)]

    assert {event["rede"] for event in events} <= {"2", "3"}
    assert all(event["serie"] == "2" for event in events)


def test_meta_event_uses_official_network_by_domain():
    expected_networks = {
        "municipio": "Municipal",
        "uf": "Pública",
        "brasil": "Pública",
    }

    for domain, expected_network in expected_networks.items():
        event = generate_single_meta_event(domain=domain, custom_id="test-id")

        assert event["domain"] == domain
        assert event["rede"] == expected_network
        assert event["meta_alfabetizacao_2030"] == 80.0
        assert event["event_type"] == "META_UPDATE"


def test_meta_event_2030_target_is_fixed_at_official_goal():
    for domain in ("municipio", "uf", "brasil"):
        for _ in range(20):
            event = generate_single_meta_event(domain=domain)

            assert event["meta_alfabetizacao_2030"] == 80.0
            assert all(
                event[f"meta_alfabetizacao_{year}"] <= 80.0
                for year in range(2024, 2030)
            )


def test_meta_event_rejects_unknown_domain():
    with pytest.raises(ValueError, match="Domínio de meta inválido"):
        generate_single_meta_event(domain="unknown")
