import logging
import queue
import threading
import time
from typing import List

from langchain_core.documents import Document
from tqdm import tqdm


class BatchInserter:
    def __init__(self, table, batch_size: int = 1000):
        self.logger = logging.getLogger(__name__)
        self.table = table
        self.batch_size = batch_size
        self.document_queue = queue.Queue()

        self.shutdown_event = threading.Event()
        self.worker_thread = None

        self.documents_added = 0
        self.documents_flushed = 0
        self.progress_lock = threading.Lock()
        self.progress_bar = None
        self.progress_bar_created = False

    def start(self):
        self.shutdown_event.clear()
        self.worker_thread = threading.Thread(target=self._flush_worker, daemon=True)
        self.worker_thread.start()

    def finish(self):
        self.shutdown_event.set()
        if self.worker_thread:
            self.worker_thread.join()
        if self.progress_bar:
            self.progress_bar.close()

    def add_document(self, doc: Document):
        self.document_queue.put(doc)
        with self.progress_lock:
            self.documents_added += 1

    def _flush_worker(self):
        while not self.shutdown_event.is_set() or not self.document_queue.empty():
            self.flush_queue()
            time.sleep(1)

    def flush_queue(self):
        docs = []
        while not self.document_queue.empty() and len(docs) < self.batch_size:
            doc = self.document_queue.get()
            docs.append(doc)

        if docs:
            try:
                self._add_documents(docs)
                with self.progress_lock:
                    self.documents_flushed += len(docs)
                    if not self.progress_bar_created:
                        self.progress_bar = tqdm(total=self.documents_added, desc="Embedding Documents", unit="doc")
                        self.progress_bar_created = True
                    if self.progress_bar:
                        self.progress_bar.total = self.documents_added
                        self.progress_bar.update(len(docs))
                        self.progress_bar.refresh()
                self.logger.debug(f"Flushed {len(docs)} documents to the vector store.")
            except Exception as e:
                self.logger.error("Error flushing document batch: %s", e)

    def _add_documents(self, documents: List[Document]):
        records = [
            {
                "id": doc.id,
                "text": doc.page_content,
                "metadata": doc.metadata,
            }
            for doc in documents
        ]
        self.table.add(records)