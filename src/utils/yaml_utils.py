import ruamel.yaml


def get_YAML():
    """Return a ruamel.yaml.YAML() that dumps multipline strings as multiple lines instead of escaping newlines."""

    def str_representer(dumper, data):
        if "\n" in data:
            return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
        return dumper.represent_scalar("tag:yaml.org,2002:str", data)

    yaml = ruamel.yaml.YAML()
    yaml.Representer.add_representer(str, str_representer)
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.width = 4096  # Prevent wrapping for long values such as bash scripts.
    return yaml
