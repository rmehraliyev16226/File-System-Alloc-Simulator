import argparse
import json
from simulator import FileSystemSimulator


def build_parser():
    parser = argparse.ArgumentParser(description="Simple File System Allocation Simulator")
    parser.add_argument("--strategy", choices=["contiguous", "fat", "inode"], default="contiguous")
    parser.add_argument("--blocks", type=int, default=32, help="Number of disk blocks")
    parser.add_argument("--workload", type=str, help="Path to workload text file")
    return parser


def run_default_demo(fs):
    print("Default demo run:")
    fs.mkdir("/docs")
    fs.create("/docs/a.txt")
    fd = fs.open("/docs/a.txt")
    fs.write(fd, 3)
    print("Read blocks:", fs.read(fd, 2))
    fs.close(fd)

    fs.link_hard("/docs/a.txt", "/docs/a_hard.txt")
    fs.link_soft("/docs/a.txt", "/docs/a_soft.txt")
    fs.delete("/docs/a.txt")

    fd2 = fs.open("/docs/a_hard.txt")
    print("Hard link still works, open fd:", fd2)
    fs.close(fd2)

    try:
        fs.open("/docs/a_soft.txt")
    except Exception as e:
        print("Soft link became broken as expected:", e)


def main():
    parser = build_parser()
    args = parser.parse_args()

    fs = FileSystemSimulator(total_blocks=args.blocks, strategy=args.strategy)

    if args.workload:
        log = fs.run_workload(args.workload)
        print("Workload execution log:")
        for item in log:
            print(" -", item)
    else:
        run_default_demo(fs)

    print("\nSummary:")
    print(json.dumps(fs.summary(), indent=2))


if __name__ == "__main__":
    main()
