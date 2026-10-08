import argparse

import pytest

from src.deployments.utils.parser import with_experiment_from_values


def _top_level(parser):
    parser.add_argument("--values", dest="values_path", default=None)
    parser.add_argument("-v", action="count", dest="verbosity", default=0)


def _parse(argv):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="experiment", required=True)
    _top_level(parser)
    for name in ("run-nodes", "nimlibp2p"):
        subparsers.add_parser(name).add_argument("--namespace", required=True)

    top_level = argparse.ArgumentParser(add_help=False)
    _top_level(top_level)
    return parser.parse_args(
        with_experiment_from_values(argv, top_level, ["run-nodes", "nimlibp2p"])
    )


@pytest.fixture
def values(tmp_path):
    path = tmp_path / "values.yaml"
    path.write_text("experiment: run-nodes\nnum_nodes: 3\n")
    return str(path)


@pytest.mark.parametrize(
    "argv",
    [
        ["--values", "{values}", "--namespace", "ns"],
        ["--namespace", "ns", "-vv", "--values", "{values}"],
    ],
)
def test_the_values_file_picks_the_experiment(values, argv):
    args = _parse([token.format(values=values) for token in argv])
    assert (args.experiment, args.namespace) == ("run-nodes", "ns")


def test_the_cli_subcommand_wins_over_the_values_file(values):
    args = _parse(["--values", values, "nimlibp2p", "--namespace", "ns"])
    assert args.experiment == "nimlibp2p"
