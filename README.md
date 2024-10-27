# Step 4: Install Required Build Tools

Make sure you have setuptools and wheel installed:

pip install setuptools wheel build

# Step 5: Build the Wheel

To create the wheel, navigate to the root directory (where pyproject.toml is located) and run:

python -m build

# Step 6: Check the Output

After running the build command, a dist directory will be created, and you should see your .whl file inside:

dist/

└── your_package_name-0.1.0-py3-none-any.whl

# Step 7: Install the Wheel Locally (Optional)

You can test the wheel installation using pip:

pip install dist/your_package_name-0.1.0-py3-none-any.whl

