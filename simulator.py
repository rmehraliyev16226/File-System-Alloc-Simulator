import json


class FileSystemSimulator:
    def __init__(self, total_blocks=32, strategy="contiguous"):
        self.total_blocks = total_blocks
        self.strategy = strategy
        self.disk = [None] * total_blocks
        self.free_blocks = set(range(total_blocks))

        # Simple hierarchical directory tree.
        self.directories = {"/": {}}

        # Real files are stored once here. Hard links share the same file id.
        self.files = {}
        self.next_file_id = 1
        self.path_to_file_id = {}

        # Soft links store path -> target path.
        self.soft_links = {}

        # Open file table: fd -> {file_id, offset, path}
        self.open_files = {}
        self.next_fd = 3

        # Journal is kept simple: list of transactions.
        self.journal = []

        # FAT-specific data.
        self.fat = [-1] * total_blocks
        self.fat_chain_traversals = 0

        # Inode-specific data.
        self.direct_limit = 4
        self.loaded_inodes = set()

        # Contiguous-specific stat.
        self.contiguous_read_seeks = 0

    # ---------------------------
    # Path / directory utilities
    # ---------------------------
    def _split_path(self, path):
        if path == "/":
            return "/", ""
        parts = [p for p in path.split("/") if p]
        parent = "/" if len(parts) == 1 else "/" + "/".join(parts[:-1])
        return parent, parts[-1]

    def _ensure_directory(self, path):
        if path in self.directories:
            return
        parent, name = self._split_path(path)
        if parent not in self.directories:
            raise ValueError(f"Parent directory does not exist: {parent}")
        self.directories[path] = {}
        self.directories[parent][name] = {"type": "dir", "path": path}

    def _resolve_file_id(self, path, follow_soft=True):
        if follow_soft and path in self.soft_links:
            target = self.soft_links[path]
            if target in self.soft_links:
                target = self.soft_links[target]
            if target not in self.path_to_file_id:
                raise ValueError(f"Path not found: {target}")
            return self.path_to_file_id[target]
        if path not in self.path_to_file_id:
            raise ValueError(f"Path not found: {path}")
        return self.path_to_file_id[path]

    # ---------------------------
    # Basic file system commands
    # ---------------------------
    def mkdir(self, path):
        self._ensure_directory(path)

    def create(self, path):
        parent, name = self._split_path(path)
        if parent not in self.directories:
            raise ValueError(f"Parent directory does not exist: {parent}")
        if path in self.path_to_file_id or path in self.soft_links:
            raise ValueError(f"Path already exists: {path}")

        file_id = self.next_file_id
        self.next_file_id += 1
        self.files[file_id] = {
            "size": 0,
            "link_count": 1,
            "blocks": [],
            "first_block": None,
            "direct_blocks": [],
            "indirect_block": None,
            "indirect_data_blocks": [],
        }
        self.path_to_file_id[path] = file_id
        self.directories[parent][name] = {"type": "file", "path": path}

    def open(self, path):
        file_id = self._resolve_file_id(path, follow_soft=True)
        fd = self.next_fd
        self.next_fd += 1
        self.open_files[fd] = {"file_id": file_id, "offset": 0, "path": path}
        if self.strategy == "inode":
            self.loaded_inodes.add(file_id)
        return fd

    def close(self, fd):
        if fd not in self.open_files:
            raise ValueError(f"Invalid fd: {fd}")
        file_id = self.open_files[fd]["file_id"]
        del self.open_files[fd]
        if self.strategy == "inode":
            still_open = any(info["file_id"] == file_id for info in self.open_files.values())
            if not still_open and file_id in self.loaded_inodes:
                self.loaded_inodes.remove(file_id)

    def write(self, fd, count):
        if fd not in self.open_files:
            raise ValueError(f"Invalid fd: {fd}")
        file_id = self.open_files[fd]["file_id"]

        if self.strategy == "contiguous":
            self._allocate_contiguous(file_id, count)
        elif self.strategy == "fat":
            self._allocate_fat(file_id, count)
        elif self.strategy == "inode":
            self._allocate_inode(file_id, count)
        else:
            raise ValueError("Unknown strategy")

        self.files[file_id]["size"] += count
        self.open_files[fd]["offset"] += count

    def read(self, fd, count):
        if fd not in self.open_files:
            raise ValueError(f"Invalid fd: {fd}")
        info = self.open_files[fd]
        file_id = info["file_id"]
        file_obj = self.files[file_id]
        start = info["offset"]
        end = min(start + count, file_obj["size"])
        if start >= end:
            return []

        blocks = self._get_file_blocks(file_id)
        result = blocks[start:end]
        info["offset"] = end

        if self.strategy == "contiguous":
            self.contiguous_read_seeks += 1
        elif self.strategy == "fat":
            self.fat_chain_traversals += len(result)

        return result

    def delete(self, path):
        if path in self.soft_links:
            self._log_delete(path, [], soft_link=True)
            parent, name = self._split_path(path)
            del self.soft_links[path]
            if parent in self.directories and name in self.directories[parent]:
                del self.directories[parent][name]
            return

        file_id = self._resolve_file_id(path, follow_soft=False)
        blocks_before = self._get_file_blocks(file_id)
        self._log_delete(path, blocks_before, soft_link=False)

        parent, name = self._split_path(path)
        if parent in self.directories and name in self.directories[parent]:
            del self.directories[parent][name]
        del self.path_to_file_id[path]

        self.files[file_id]["link_count"] -= 1
        if self.files[file_id]["link_count"] <= 0:
            self._free_file_blocks(file_id)
            if file_id in self.loaded_inodes:
                self.loaded_inodes.remove(file_id)
            del self.files[file_id]

    def link_hard(self, src, dst):
        file_id = self._resolve_file_id(src, follow_soft=True)
        parent, name = self._split_path(dst)
        if parent not in self.directories:
            raise ValueError(f"Parent directory does not exist: {parent}")
        self.path_to_file_id[dst] = file_id
        self.files[file_id]["link_count"] += 1
        self.directories[parent][name] = {"type": "file", "path": dst}

    def link_soft(self, src, dst):
        parent, name = self._split_path(dst)
        if parent not in self.directories:
            raise ValueError(f"Parent directory does not exist: {parent}")
        self.soft_links[dst] = src
        self.directories[parent][name] = {"type": "symlink", "path": dst}

    # ---------------------------
    # Allocation methods
    # ---------------------------
    def _allocate_contiguous(self, file_id, count):
        file_obj = self.files[file_id]
        current = file_obj["blocks"]

        if not current:
            start = self._find_contiguous_run(count)
            if start is None:
                raise ValueError("No contiguous space available")
            new_blocks = list(range(start, start + count))
        else:
            last = current[-1]
            new_blocks = list(range(last + 1, last + 1 + count))
            if any(b >= self.total_blocks or b not in self.free_blocks for b in new_blocks):
                raise ValueError("Cannot extend file contiguously")

        for b in new_blocks:
            self.disk[b] = file_id
            self.free_blocks.remove(b)
        file_obj["blocks"].extend(new_blocks)

    def _find_contiguous_run(self, count):
        run = 0
        start = 0
        for i in range(self.total_blocks):
            if i in self.free_blocks:
                if run == 0:
                    start = i
                run += 1
                if run == count:
                    return start
            else:
                run = 0
        return None

    def _allocate_fat(self, file_id, count):
        file_obj = self.files[file_id]
        if len(self.free_blocks) < count:
            raise ValueError("Not enough free blocks")

        new_blocks = sorted(list(self.free_blocks))[:count]
        for b in new_blocks:
            self.disk[b] = file_id
            self.free_blocks.remove(b)

        if file_obj["first_block"] is None:
            file_obj["first_block"] = new_blocks[0]
        else:
            last = file_obj["blocks"][-1]
            self.fat[last] = new_blocks[0]

        for i in range(len(new_blocks) - 1):
            self.fat[new_blocks[i]] = new_blocks[i + 1]
        self.fat[new_blocks[-1]] = -2
        file_obj["blocks"].extend(new_blocks)
        self.fat_chain_traversals += len(new_blocks)

    def _allocate_inode(self, file_id, count):
        file_obj = self.files[file_id]
        if len(self.free_blocks) < count:
            raise ValueError("Not enough free blocks")

        for _ in range(count):
            b = min(self.free_blocks)
            self.free_blocks.remove(b)
            self.disk[b] = file_id
            file_obj["blocks"].append(b)

            if len(file_obj["direct_blocks"]) < self.direct_limit:
                file_obj["direct_blocks"].append(b)
            else:
                if file_obj["indirect_block"] is None:
                    if not self.free_blocks:
                        raise ValueError("No space for indirect block")
                    ptr_block = min(self.free_blocks)
                    self.free_blocks.remove(ptr_block)
                    self.disk[ptr_block] = f"inode_ptr_{file_id}"
                    file_obj["indirect_block"] = ptr_block
                file_obj["indirect_data_blocks"].append(b)

    def _get_file_blocks(self, file_id):
        file_obj = self.files[file_id]
        if self.strategy == "fat" and file_obj["first_block"] is not None:
            result = []
            current = file_obj["first_block"]
            while current != -2 and current is not None:
                result.append(current)
                nxt = self.fat[current]
                self.fat_chain_traversals += 1
                current = None if nxt == -1 else nxt
            return result
        return list(file_obj["blocks"])

    def _free_file_blocks(self, file_id):
        file_obj = self.files[file_id]
        for b in file_obj["blocks"]:
            self.disk[b] = None
            self.free_blocks.add(b)
            if self.strategy == "fat":
                self.fat[b] = -1
        if self.strategy == "inode" and file_obj["indirect_block"] is not None:
            ptr = file_obj["indirect_block"]
            self.disk[ptr] = None
            self.free_blocks.add(ptr)

    # ---------------------------
    # Journal and statistics
    # ---------------------------
    def _log_delete(self, path, blocks, soft_link=False):
        entry = {
            "operation": "delete",
            "path": path,
            "type": "soft_link" if soft_link else "file",
            "steps": [
                "remove directory entry",
                "release metadata",
                "return blocks",
            ],
            "blocks_before_delete": blocks,
        }
        self.journal.append(entry)

    def _contiguous_stats(self):
        extents = []
        current = 0
        for i in range(self.total_blocks):
            if i in self.free_blocks:
                current += 1
            else:
                if current > 0:
                    extents.append(current)
                current = 0
        if current > 0:
            extents.append(current)

        total_free = len(self.free_blocks)
        largest = max(extents) if extents else 0
        fragmentation = 0.0
        if total_free > 0:
            fragmentation = round(1 - (largest / total_free), 4)
        return {
            "free_extents": len(extents),
            "largest_free_extent": largest,
            "external_fragmentation": fragmentation,
        }

    def summary(self):
        stats = {}
        if self.strategy == "contiguous":
            stats = self._contiguous_stats()
        elif self.strategy == "fat":
            stats = {"fat_memory_overhead_bytes": self.total_blocks * 4}
        elif self.strategy == "inode":
            stats = {"loaded_inodes": len(self.loaded_inodes), "inode_memory_overhead_bytes": len(self.loaded_inodes) * 64}

        result = {
            "strategy": self.strategy,
            "disk_blocks": self.total_blocks,
            "free_blocks": len(self.free_blocks),
            "used_blocks": self.total_blocks - len(self.free_blocks),
            "journal_entries": len(self.journal),
            "strategy_stats": stats,
        }
        if self.strategy == "contiguous":
            result["contiguous_read_seeks"] = self.contiguous_read_seeks
        elif self.strategy == "fat":
            result["fat_chain_traversals"] = self.fat_chain_traversals
        return result

    # ---------------------------
    # Workload support
    # ---------------------------
    def execute_command(self, line):
        parts = line.strip().split()
        if not parts or parts[0].startswith("#"):
            return None

        cmd = parts[0].upper()
        if cmd == "MKDIR":
            self.mkdir(parts[1])
            return f"MKDIR {parts[1]}"
        if cmd == "CREATE":
            self.create(parts[1])
            return f"CREATE {parts[1]}"
        if cmd == "OPEN":
            fd = self.open(parts[1])
            return f"OPEN {parts[1]} -> fd {fd}"
        if cmd == "CLOSE":
            fd = int(parts[1])
            self.close(fd)
            return f"CLOSE fd {fd}"
        if cmd == "WRITE":
            fd = int(parts[1])
            count = int(parts[2])
            self.write(fd, count)
            return f"WRITE fd {fd} {count}"
        if cmd == "READ":
            fd = int(parts[1])
            count = int(parts[2])
            data = self.read(fd, count)
            return f"READ fd {fd} {count} -> {data}"
        if cmd == "DELETE":
            self.delete(parts[1])
            return f"DELETE {parts[1]}"
        if cmd == "LINK_HARD":
            self.link_hard(parts[1], parts[2])
            return f"LINK_HARD {parts[1]} {parts[2]}"
        if cmd == "LINK_SOFT":
            self.link_soft(parts[1], parts[2])
            return f"LINK_SOFT {parts[1]} {parts[2]}"

        raise ValueError(f"Unknown command: {line}")

    def run_workload(self, filename):
        log = []
        with open(filename, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                log.append(self.execute_command(line))
        return log
