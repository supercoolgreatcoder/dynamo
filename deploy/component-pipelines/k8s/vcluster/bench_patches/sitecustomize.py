"""AIPerf 0.12.0 benchmark-only workaround for concurrent mmap reads.

Loaded through PYTHONPATH in the dedicated fixed-reader Mooncake series. The
upstream get_conversation method calls seek() and read() on a shared mmap;
concurrent requests can move the cursor between those calls. Index-based
slicing reads the same byte range without mutating that cursor. Do not mount
this into serving components or into earlier benchmark series.
"""

from aiperf.dataset.memory_map_utils import (
    MemoryMapDatasetClient,
    MemoryMapFormat,
    MemoryMapSerializationError,
)


def _cursor_free_get_conversation(self: MemoryMapDatasetClient, conversation_id: str):
    if self.index.format == MemoryMapFormat.PAYLOAD_BYTES:
        raise MemoryMapSerializationError(
            f"Cannot retrieve Conversation '{conversation_id}' in payload_bytes "
            "format. Use get_payload_bytes() instead."
        )
    if conversation_id not in self.index.offsets:
        raise KeyError(f"Conversation '{conversation_id}' not found in dataset")

    offset_info = self.index.offsets[conversation_id]
    start = offset_info.offset
    end = start + offset_info.size
    conversation_bytes = self.data_mmap[start:end]
    if len(conversation_bytes) != offset_info.size:
        raise MemoryMapSerializationError(
            f"Short mmap read for Conversation '{conversation_id}': "
            f"got {len(conversation_bytes)} bytes, expected {offset_info.size}"
        )
    return self._deserialize_conversation(conversation_bytes)


MemoryMapDatasetClient.get_conversation = _cursor_free_get_conversation
