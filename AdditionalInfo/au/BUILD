python_sources()

pex_binary(
    name="main",
    entry_point="main.py",
    dependencies=[
        "projects/bank_data",
        "projects/aws:aws_sdk",
        "projects/slack:slack_build_update",
    ],
)

pex_binary(
    name="registry",
    entry_point="registry.py",
    dependencies=[
        "projects/aws:aws_sdk",
    ]
)
