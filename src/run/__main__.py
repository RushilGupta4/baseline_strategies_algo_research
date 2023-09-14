import utils
import os, sys


def run(name):
    module_name = f"{name}"
    strategy = __import__(module_name)
    strategy.run()


protected_files = ["__init__.py", "__main__.py", "runner.py"]


def main():
    base_output_path = "output"
    os.makedirs(base_output_path, exist_ok=True)

    current_dir = os.path.dirname(os.path.realpath(__file__))

    files = utils.get_files_in_dir(current_dir)
    for file in protected_files:
        if file in files:
            files.remove(file)

    argv = sys.argv[1:]
    if len(argv) < 1:
        print("Please provide a strategy name (or ALL)")
        exit(1)

    line = "*" * 60

    if len(argv) == 1 and argv[0] == "ALL":
        for file in files:
            run(file[:-3])

        print()
        print(line)
        print()

        return

    # Here we want to run the specified ones
    for arg in argv:
        file = f"{arg}.py"
        if file not in files:
            print(f"\n{line}\nStrategy {file} not found\n{line}\n")
            continue

        run(arg)


if __name__ == "__main__":
    main()
