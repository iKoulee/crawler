# Coding Standards and Development Guidelines

This document outlines the coding standards and development practices to be followed when contributing to this project. These guidelines aim to maintain code quality, consistency, and testability across the codebase.

## Python Standards

### PEP 8 Compliance

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guide for Python code.
- Use 4 spaces for indentation (no tabs).
- Keep lines to a maximum of 79 characters for code and 72 for comments and docstrings.
- Use appropriate naming conventions:
  - `snake_case` for variables, functions, methods, and modules
  - `PascalCase` for classes
  - `SCREAMING_SNAKE_CASE` for constants
- Use meaningful and descriptive names.
- Add appropriate whitespace around operators and after commas.
- Use consistent import ordering:
  1. Standard library imports
  2. Related third-party imports
  3. Local application/library specific imports
- Separate import groups with a blank line.

### Type Hints

- Always include type hints for function parameters and return values following [PEP 484](https://www.python.org/dev/peps/pep-0484/).
- Use the typing module for complex types:
  ```python
  from typing import List, Dict, Tuple, Optional, Union, Any, Callable
  ```
- For function annotations:
  ```python
  def process_data(input_data: List[Dict[str, Any]]) -> Dict[str, float]:
      # Function body
      return result
  ```
- For variable annotations:
  ```python
  names: List[str] = ["Alice", "Bob"]
  age_map: Dict[str, int] = {"Alice": 30, "Bob": 25}
  ```
- Use Optional for parameters that could be None:
  ```python
  def process_user(user_id: Optional[int] = None) -> None:
      # Function body
      pass
  ```
- For class variables, use annotations:
  ```python
  class User:
      name: str
      age: int
      is_active: bool = True
  ```

## Standards for Other Languages

### JavaScript/TypeScript

- Use camelCase for variables, functions, and methods.
- Use PascalCase for classes and component names.
- Use 2-space indentation.
- End statements with semicolons.
- Use ES6+ features when available.
- Use JSDoc comments for documentation.
- For TypeScript, use explicit type annotations.

### HTML/CSS

- Use 2-space indentation.
- Use lowercase for HTML elements and attributes.
- Use kebab-case for CSS class names.
- Follow BEM (Block Element Modifier) methodology for CSS classes when applicable.

### SQL

- Use UPPERCASE for SQL keywords.
- Use snake_case for table and column names.
- Indent SQL statements for readability.
- Use meaningful table and column names.

## Clean Code Principles (Kent Beck Style)

### General Principles

- **Simplicity**: Write the simplest code that works.
- **Clarity**: Code should be readable and intention-revealing.
- **DRY (Don't Repeat Yourself)**: Eliminate duplication.
- **YAGNI (You Aren't Gonna Need It)**: Don't implement features until they are needed.
- **Small Functions/Methods**: Keep functions and methods small and focused.
- **Single Responsibility Principle**: Each function, class, or module should have one responsibility.
- **Meaningful Names**: Choose descriptive and intention-revealing names.
- **Comments**: Use comments only when necessary, prefer self-documenting code.
- **Error Handling**: Handle errors gracefully and appropriately.

### Test-Driven Development (TDD)

Follow Kent Beck's rigorous TDD approach:

1. **Red**: Write a failing test that defines the functionality you want to implement.
2. **Green**: Write the simplest code that makes the test pass.
3. **Refactor**: Clean up the code while keeping the tests passing.

#### TDD Practices

- Write the test before writing the implementation code.
- Start with the simplest test case.
- Add complexity gradually with additional tests.
- Maintain a fast test cycle (write test, see it fail, make it pass, refactor).
- Test one behavior at a time.
- Keep tests independent of each other.
- Focus on behavior, not implementation details.
- Run tests frequently.

## Implementation Guidelines

### Only Write Code Needed to Pass Tests

- Implement only the functionality required to pass the current tests.
- Do not add speculative features or "nice-to-have" functionality without corresponding tests.
- Focus on making the current tests pass before moving on to new functionality.
- If a requirement changes, first update the tests, then update the implementation.

### Specific Implementation Practices

- Start with the simplest implementation that could possibly work.
- Incrementally improve the implementation as more tests are added.
- Refactor code regularly to maintain clean design.
- When refactoring, ensure all tests continue to pass.

## Documentation

- Document public APIs with clear descriptions of parameters, return values, and exceptions.
- Include examples in documentation where appropriate.
- Keep documentation up-to-date with code changes.
- Document design decisions and architectural patterns used.

## Version Control Practices

- Write descriptive commit messages that explain why changes were made.
- Keep commits focused on a single logical change.
- Reference relevant issues or tickets in commit messages.
- Create feature branches for new work.
- Pull and rebase frequently to minimize merge conflicts.

## Conclusion

These guidelines aim to maintain a high-quality, consistent, and testable codebase. By following these standards, we ensure that our code is maintainable, extensible, and reliable. Remember that the ultimate goal is to produce working software that meets the requirements while remaining adaptable to future changes.
