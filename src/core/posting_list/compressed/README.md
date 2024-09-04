## CompressedPostingList

为了节省空间，对 PostingList 进行压缩

压缩路线分为两个部分:
- 使用 `bitpacker` 对 row_id 进行压缩
- 使用自定义的量化方式对 weight 进行压缩

