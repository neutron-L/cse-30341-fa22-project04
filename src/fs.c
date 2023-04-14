/* fs.c: SimpleFS file system */

#include "sfs/fs.h"
#include "sfs/logging.h"
#include "sfs/utils.h"

#include <stdio.h>
#include <string.h>

/* Internal Functions */
static bool    fs_load_inode(FileSystem *fs, size_t inode_number, Inode *node);
static bool    fs_save_inode(FileSystem *fs, size_t inode_number, Inode *node);
static void    fs_initialize_free_block_bitmap(FileSystem *fs);
static ssize_t fs_allocate_free_block(FileSystem *fs);
static void fs_release_free_block(FileSystem *fs, size_t block_number);


/* External Functions */

/**
 * Debug FileSystem by doing the following:
 *
 *  1. Read SuperBlock and report its information.
 *
 *  2. Read Inode Table and report information about each Inode.
 *
 * @param       disk        Pointer to Disk structure.
 **/
void    fs_debug(Disk *disk) {
    Block block;

    /* Read SuperBlock */
    if (disk_read(disk, 0, block.data) == DISK_FAILURE) {
        fprintf(stderr, "Fail to read super block\n");
        return;
    }

    printf("SuperBlock:\n");
    printf("    magic number is %s\n",
        (block.super.magic_number == MAGIC_NUMBER) ? "valid" : "invalid");
    printf("    %u blocks\n"         , block.super.blocks);
    printf("    %u inode blocks\n"   , block.super.inode_blocks);
    printf("    %u inodes\n"         , block.super.inodes);

    /* Read Inodes */
    size_t nums = block.super.inode_blocks;

    // 读取inode块
    for (size_t i = 0; i < block.super.inode_blocks; ++i)
    {
        Block inode_block;
        if (disk_read(disk, i + 1, inode_block.data) == DISK_FAILURE)
        {
            fprintf(stderr, "Fail to read block %d\n", i + 1);
            return;
        }
        
        for (size_t j = 0; j < INODES_PER_BLOCK; ++j)
        {
            Inode * pi = &inode_block.inodes[j];
            // print valid inode的信息
            if (pi->valid)
            {
                printf("Inode: %d\n", i * INODES_PER_BLOCK + j);
                printf("    size: %d bytes\n", pi->size);
                for (size_t k = 0; k < POINTERS_PER_INODE && pi->direct[k]; ++k)
                    printf("    direct blocks: %d\n", pi->direct[k]);
                // 存在indirect inode
                if (pi->indirect)
                {
                    printf("    indirect blocks: ");
                    Block indirect_block;

                    if (disk_read(disk, pi->indirect, indirect_block.data) == DISK_FAILURE)
                    {
                        fprintf(stderr, "Fail to read indirect block %d\n", pi->indirect);
                        return;
                    }
                    for (size_t k = 0; k < POINTERS_PER_BLOCK && indirect_block.pointers[k]; ++k)
                        printf("%d ", indirect_block.pointers[k]);
                    printf("\n");
                }
                --nums;
            }
        }

        // 如果没有inode_block了，exit
        if (!nums)
            break;
    }
}

/**
 * Format Disk by doing the following:
 *
 *  1. Write SuperBlock (with appropriate magic number, number of blocks,
 *  number of inode blocks, and number of inodes).
 *
 *  2. Clear all remaining blocks.
 *
 * Note: Do not format a mounted Disk!
 *
 * @param       fs      Pointer to FileSystem structure.
 * @param       disk    Pointer to Disk structure.
 * @return      Whether or not all disk operations were successful.
 **/
bool    fs_format(FileSystem *fs, Disk *disk) {
    (void)fs;

    size_t inodes_size = (disk->blocks + 9) / 10;

    // clear inode blocks
    char * data = (char *)calloc(BLOCK_SIZE, sizeof(char));
    for (size_t i = 0; i < inodes_size; ++i)
        if (disk_write(disk, i + 1, data) == DISK_FAILURE)
        {
            error("Fail to clear inode block %d\n", i + 1);
            return false;
        }

    // Write SuperBlock
    uint32_t * ptr = (uint32_t *)data;
    ptr[0] = MAGIC_NUMBER;
    ptr[1] = disk->blocks;
    ptr[2] = inodes_size;
    ptr[3] = disk->blocks - 1 - inodes_size;
    if (disk_write(disk, 0, data) == DISK_FAILURE)
    {
        error("Fail to init super block\n");
        return false;
    }

    return true;
}

/**
 * Mount specified FileSystem to given Disk by doing the following:
 *
 *  1. Read and check SuperBlock (verify attributes).
 *
 *  2. Verify and record FileSystem disk attribute.
 *
 *  3. Copy SuperBlock to FileSystem meta data attribute
 *
 *  4. Initialize FileSystem free blocks bitmap.
 *
 * Note: Do not mount a Disk that has already been mounted!
 *
 * @param       fs      Pointer to FileSystem structure.
 * @param       disk    Pointer to Disk structure.
 * @return      Whether or not the mount operation was successful.
 **/
bool    fs_mount(FileSystem *fs, Disk *disk) {
    if (disk)
    {
        fs->disk = disk;
        // read super block
        if (disk_read(disk, 0, &fs->meta_data) == DISK_FAILURE)
        {
            error("Fail to read super block\n");
            return false;
        }

        // check magic number
        if (fs->meta_data.magic_number != MAGIC_NUMBER)
        {
            error("Magic number is invalid\n");
            return false;
        }

        // initialize free bitmap
        fs_initialize_free_block_bitmap(fs);

        return true;
    }

    return false;
}

/**
 * Unmount FileSystem from internal Disk by doing the following:
 *
 *  1. Set FileSystem disk attribute.
 *
 *  2. Release free blocks bitmap.
 *
 * @param       fs      Pointer to FileSystem structure.
 **/
void    fs_unmount(FileSystem *fs) {
    if (fs)
    {
        fs->disk = NULL;
        free(fs->free_blocks);
        fs->free_blocks = NULL;
    }
}

/**
 * Allocate an Inode in the FileSystem Inode table by doing the following:
 *
 *  1. Search Inode table for free inode.
 *
 *  2. Reserve free inode in Inode table.
 *
 * Note: Be sure to record updates to Inode table to Disk.
 *
 * @param       fs      Pointer to FileSystem structure.
 * @return      Inode number of allocated Inode.
 **/
ssize_t fs_create(FileSystem *fs) {
    Block block;

    // 访问每一个inode块
    for (size_t i = 0; i < fs->meta_data.inode_blocks; ++i)
    {
        // 读入inode 块
        if (disk_read(fs->disk, i + 1, block.data) == DISK_FAILURE)
        {
            error("Fail to read inode block\n");
            return -1;
        }
        
        // 访问每一个inode
        for (size_t j = 0; j < INODES_PER_BLOCK; ++j)
        {
            if (!block.inodes[j].valid)
            {
                block.inodes[j].valid = 1;
                // write back
                if (disk_write(fs->disk, i + 1, block.data) == DISK_FAILURE)
                {
                    error("Fail to write inode block back\n");
                    return -1;
                }

                return i * INODES_PER_BLOCK + j;
            }
        }
    }

    return -1;
}

/**
 * Remove Inode and associated data from FileSystem by doing the following:
 *
 *  1. Load and check status of Inode.
 *
 *  2. Release any direct blocks.
 *
 *  3. Release any indirect blocks.
 *
 *  4. Mark Inode as free in Inode table.
 *
 * @param       fs              Pointer to FileSystem structure.
 * @param       inode_number    Inode to remove.
 * @return      Whether or not removing the specified Inode was successful.
 **/
bool    fs_remove(FileSystem *fs, size_t inode_number) {
    // load and check status of Inode
    Inode inode;
    size_t i;

    if (!fs_load_inode(fs, inode_number, &inode) || !inode.valid)
    {
        error("Fail to load inode %d or %d inode is invalid\n", inode_number);
        return false;
    }


    // release direct blocks
    for (i = 0; i < POINTERS_PER_INODE && inode.direct[i]; ++i)
        fs_release_free_block(fs, inode.direct[i]);

    // release indirect blocks
    if (inode.indirect)
    {
        // read indirect block
        Block block;
        disk_read(fs, inode.indirect, block.pointers);

        for (i = 0; i < POINTERS_PER_BLOCK && block.pointers[i]; ++i)
            fs_release_free_block(fs, block.pointers[i]);
        fs_release_free_block(fs, inode.indirect);
    }

    
    // mark inode as free and save
    memset(&inode, 0, sizeof(inode));
    if (!fs_save_inode(fs, inode_number, &inode))
    {
        error("Fail to save inode %d\n", inode_number);
        return false;
    }

    return true;
}

/**
 * Return size of specified Inode.
 *
 * @param       fs              Pointer to FileSystem structure.
 * @param       inode_number    Inode to remove.
 * @return      Size of specified Inode (-1 if does not exist).
 **/
ssize_t fs_stat(FileSystem *fs, size_t inode_number) {
    Inode inode;
    return fs_load_inode(fs, inode_number, &inode) ? inode.size : -1; 
}

/**
 * Read from the specified Inode into the data buffer exactly length bytes
 * beginning from the specified offset by doing the following:
 *
 *  1. Load Inode information.
 *
 *  2. Continuously read blocks and copy data to buffer.
 *
 *  Note: Data is read from direct blocks first, and then from indirect blocks.
 *
 * @param       fs              Pointer to FileSystem structure.
 * @param       inode_number    Inode to read data from.
 * @param       data            Buffer to copy data to.
 * @param       length          Number of bytes to read.
 * @param       offset          Byte offset from which to begin reading.
 * @return      Number of bytes read (-1 on error).
 **/
ssize_t fs_read(FileSystem *fs, size_t inode_number, char *data, size_t length, size_t offset) {
    Inode inode;

    if (fs_load_inode(fs, inode_number, &inode))
    {
        // 暂存数据块
        Block block;
        size_t bytes_read = 0;

        // 读取direct block
        size_t i = offset / BLOCK_SIZE;
        offset %= BLOCK_SIZE;
        while (i < POINTERS_PER_INODE && inode.direct[i] && bytes_read < length)
        {
            if (!disk_read(fs, inode.direct[i], block.data))
            {
                error("Fail to read block %d\n", inode.direct[i]);
                return -1;
            }
            // 拷贝数据
            size_t sz = min(BLOCK_SIZE - offset, length - bytes_read);
            memcpy(data + bytes_read, block.data + offset % BLOCK_SIZE, sz);
            bytes_read += sz;

            ++i;
            offset = 0;
        }

        // 读取indirect block
        if (bytes_read < length && inode.indirect)
        {
            // 读取indirect block
            Block indirect_block;

            if (!disk_read(fs, inode.indirect, block.data))
            {
                error("Fail to read block %d\n", inode.indirect);
                return -1;
            }

            for (size_t i = 0; i < POINTERS_PER_BLOCK && indirect_block.pointers[i] && bytes_read < length; ++i)
            {
                if (!disk_read(fs, indirect_block.pointers[i], block.data))
                {
                    error("Fail to read block %d\n", indirect_block.pointers[i]);
                    return -1;
                }   

                // 拷贝数据
                size_t sz = min(BLOCK_SIZE, length - bytes_read);
                memcpy(data + bytes_read, block.data + offset % BLOCK_SIZE, sz);
                bytes_read += sz;
            }
        }

        return bytes_read;
    }

    return -1;
}

/**
 * Write to the specified Inode from the data buffer exactly length bytes
 * beginning from the specified offset by doing the following:
 *
 *  1. Load Inode information.
 *
 *  2. Continuously copy data from buffer to blocks.
 *
 *  Note: Data is read from direct blocks first, and then from indirect blocks.
 *
 * @param       fs              Pointer to FileSystem structure.
 * @param       inode_number    Inode to write data to.
 * @param       data            Buffer with data to copy
 * @param       length          Number of bytes to write.
 * @param       offset          Byte offset from which to begin writing.
 * @return      Number of bytes read (-1 on error).
 **/
ssize_t fs_write(FileSystem *fs, size_t inode_number, char *data, size_t length, size_t offset) {
    Inode inode;

    if (fs_load_inode(fs, inode_number, &inode))
    {
        // 数据块
        Block block;
        size_t bytes_write = 0;

        // 读取direct block
        size_t i = offset / BLOCK_SIZE;
        offset %= BLOCK_SIZE;
        while (i < POINTERS_PER_INODE && inode.direct[i] && bytes_write < length)
        {
            if (!disk_read(fs, inode.direct[i], block.data))
            {
                error("Fail to read block %d\n", inode.direct[i]);
                return -1;
            }
            // 拷贝数据
            size_t sz = min(BLOCK_SIZE - offset, length - bytes_write);
            memcpy(block.data + offset % BLOCK_SIZE, data + bytes_write, sz);
            bytes_write += sz;

             // write back
            if (!disk_write(fs, i, block.data))
            {

            }

            ++i;
            offset = 0;
        }

        // 读取indirect block
        if (bytes_write < length && inode.indirect)
        {
            // 读取indirect block
            Block indirect_block;

            if (!disk_read(fs, inode.indirect, block.data))
            {
                error("Fail to read block %d\n", inode.indirect);
                return -1;
            }

            for (size_t i = 0; i < POINTERS_PER_BLOCK && indirect_block.pointers[i] && bytes_write < length; ++i)
            {
                if (!disk_read(fs, indirect_block.pointers[i], block.data))
                {
                    error("Fail to read block %d\n", indirect_block.pointers[i]);
                    return -1;
                }   

                // 拷贝数据
                size_t sz = min(BLOCK_SIZE, length - bytes_write);
                memcpy(block.data + offset, data + bytes_write, sz);
                bytes_write += sz;

                 // write back
                if (!disk_write(fs, i, block.data))
                {

                }
            }
        }

        return bytes_write;
    }

    return -1;
}

/* vim: set expandtab sts=4 sw=4 ts=8 ft=c: */

/* Internal Functions */
static bool    fs_load_inode(FileSystem *fs, size_t inode_number, Inode *node)
{
    // inode block number
    size_t inode_block_number = 1 + inode_number / INODES_PER_BLOCK;
    Block block;

    if (!disk_read(fs, inode_block_number, block.inodes))
    {

    }

    inode_number %= INODES_PER_BLOCK;

    memcpy(node, &block.inodes[inode_number], sizeof(Inode));

    return true;
}


static bool    fs_save_inode(FileSystem *fs, size_t inode_number, Inode *node)
{
// inode block number
    size_t inode_block_number = 1 + inode_number / INODES_PER_BLOCK;
    Block block;

    if (!disk_read(fs, inode_block_number, block.inodes))
    {

    }

    inode_number %= INODES_PER_BLOCK;

    memcpy(&block.inodes[inode_number], node, sizeof(Inode));

    if (!disk_write(fs, inode_block_number, block.data))
    {

    }
    
    return true;
}


static void    fs_initialize_free_block_bitmap(FileSystem *fs)
{
    fs->free_blocks = (bool *)malloc(fs->disk->blocks * sizeof(bool));

    size_t i;
    // super block and inode blocks are not free
    for (size_t i = 0; i < 1 + fs->meta_data.inode_blocks; ++i)
        fs->free_blocks[i] = false;
    // remain blocks are free
    while (i < fs->meta_data.blocks)
        fs->free_blocks[i++] = true;

    // 找到磁盘中已经使用的块 set false
    // 访问每一个inode块
    Block block;
    for (size_t i = 0; i < fs->meta_data.inode_blocks; ++i)
    {
        // 读入inode 块
        if (disk_read(fs->disk, i + 1, block.data) == DISK_FAILURE)
        {
            error("Fail to read inode block\n");
            return -1;
        }
        
        // 访问每一个inode
        for (size_t j = 0; j < INODES_PER_BLOCK; ++j)
        {
            Inode *pi = &block.inodes[j];
            if (pi->valid)
            {
                for (size_t k = 0; k < INODES_PER_BLOCK && pi->direct[k]; ++k)
                    fs->free_blocks[pi->direct[k]] = false;
                if (pi->indirect)
                {
                    fs->free_blocks[pi->direct[pi->indirect]] = false;

                    Block indirect_block;
                    // 读入inode 块
                    if (disk_read(fs->disk, i + 1, indirect_block.pointers) == DISK_FAILURE)
                    {
                        error("Fail to read inode block\n");
                        return -1;
                    }
                    for (size_t k = 0; k < POINTERS_PER_BLOCK && indirect_block.pointers[k]; ++k)
                        fs->free_blocks[indirect_block.pointers[k]] = false;
                }
            }
        }
    }
}


static ssize_t fs_allocate_free_block(FileSystem *fs)
{
    size_t i = 1 + fs->meta_data.inode_blocks;
    while (i < fs->disk->blocks && !fs->free_blocks[i])
        ++i;
    fs->free_blocks[i] = false;
    return i;
}


static void fs_release_free_block(FileSystem *fs, size_t block_number)
{
    fs->free_blocks[block_number] = true;
}