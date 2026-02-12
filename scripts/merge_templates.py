import argparse
from pathlib import Path
from typing import Dict, Any, Iterable

import yaml
from yaml.nodes import MappingNode, ScalarNode, SequenceNode


class Tagged:
    def __init__(self, tag: str, value):
        self.tag = tag
        self.value = value


class CfnLoader(yaml.SafeLoader):
    pass


class CfnDumper(yaml.SafeDumper):
    pass


def _construct_tagged(loader: yaml.Loader, tag_suffix: str, node):
    tag = f"!{tag_suffix}"
    if isinstance(node, ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, SequenceNode):
        value = loader.construct_sequence(node)
    elif isinstance(node, MappingNode):
        value = loader.construct_mapping(node)
    else:
        value = loader.construct_object(node)
    return Tagged(tag, value)


def _represent_tagged(dumper: yaml.Dumper, data: "Tagged"):
    if isinstance(data.value, dict):
        return dumper.represent_mapping(data.tag, data.value)
    if isinstance(data.value, list):
        return dumper.represent_sequence(data.tag, data.value)
    return dumper.represent_scalar(data.tag, str(data.value))


CfnLoader.add_multi_constructor("!", _construct_tagged)
CfnDumper.add_representer(Tagged, _represent_tagged)


def _load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.load(handle, Loader=CfnLoader) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return data


def _merge_section(dest: Dict[str, Any], src: Dict[str, Any], section: str, path: Path) -> None:
    if section not in src:
        return
    value = src[section]
    if not isinstance(value, dict):
        raise ValueError(f"{path} section {section} must be a mapping")
    dest.setdefault(section, {})
    for key, item in value.items():
        if key in dest[section]:
            raise ValueError(f"Duplicate {section} key {key} in {path}")
        dest[section][key] = item


def merge_templates(base_path: Path, fragment_paths: Iterable[Path], out_path: Path) -> None:
    base = _load_yaml(base_path)
    merged = dict(base)

    for fragment in fragment_paths:
        data = _load_yaml(fragment)
        for section in ["Parameters", "Conditions", "Resources", "Outputs"]:
            _merge_section(merged, data, section, fragment)

    with out_path.open("w", encoding="utf-8") as handle:
        yaml.dump(merged, handle, Dumper=CfnDumper, sort_keys=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge CloudFormation template fragments.")
    parser.add_argument("--base", required=True)
    parser.add_argument("--fragments", nargs="+", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    base_path = Path(args.base)
    fragment_paths = [Path(p) for p in args.fragments]
    out_path = Path(args.out)

    merge_templates(base_path, fragment_paths, out_path)


if __name__ == "__main__":
    main()
