# Contributing to snowforge-py

We love your input! We want to make contributing to snowforge-py as easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features
- Becoming a maintainer

## Development Process

We use GitHub to host code, to track issues and feature requests, as well as accept pull requests.

1. Fork the repo and create your branch from `main`
2. If you've added code that should be tested, add tests
3. If you've changed APIs, update the documentation
4. Ensure the test suite passes
5. Make sure your code lints
6. Issue that pull request!

## Development Setup

1. Clone your fork:

```bash
git clone https://github.com/your-username/snowforge-py.git
cd snowforge-py
```

2. Install poetry

```bash
pip install poetry
```

3. Install dependencies

```bash
poetry install
```

4. Create a new branch

```bash
git checkout -b feature/your-feature-name
```


## Code Style

- We use [black](https://github.com/psf/black) for Python code formatting
- We use [isort](https://pycqa.github.io/isort/) for import sorting
- We use [flake8](https://flake8.pycqa.org/) for style guide enforcement
- We use [mypy](http://mypy-lang.org/) for static type checking

## Testing

- Write tests for all new features
- Ensure all tests pass before submitting a PR
- Use pytest for testing

## Documentation

- Update the README.md if needed
- Add docstrings to all public methods and classes
- Update the examples if you add new features
- Keep the CHANGELOG.md up to date

## Pull Request Process

1. Update the README.md with details of changes to the interface
2. Update the CHANGELOG.md with notes on your changes
3. The PR will be merged once you have the sign-off of at least one maintainer

## License

By contributing, you agree that your contributions will be licensed under its GPL-3.0 License.

## Local Testing

To run tests locally using Act (GitHub Actions locally):
