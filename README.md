# File-System-Alloc-Simulator

This project is a simple Python simulator for three file allocation methods:
- Contiguous allocation
- FAT allocation
- I-node allocation

It also supports:
- Hierarchical directories
- Hard links
- Soft links
- Open, close, read, write, create, delete
- Basic journaling for delete operations

## Files
- `main.py` -> program entry point
- `simulator.py` -> simulator logic
- `workload_example.txt` -> sample workload

## How to run

Run default demo:

```bash
python main.py --strategy contiguous
python main.py --strategy fat
python main.py --strategy inode
```

Run with workload:

```bash
python main.py --strategy contiguous --workload workload_example.txt
python main.py --strategy fat --workload workload_example.txt
python main.py --strategy inode --workload workload_example.txt
```

Run with custom disk size:

```bash
python main.py --strategy contiguous --blocks 32 --workload workload_example.txt
```

## Notes
- File sizes are measured in blocks, not bytes.
- The FAT memory overhead is simulated as 4 bytes per block entry.
- Inode memory overhead is simulated as 64 bytes per loaded inode.
- Only one indirect pointer block is used in the inode strategy.
