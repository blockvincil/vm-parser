"""Where parsed batches go.
DbSink = production (Postgres).
FileSink = local testing (JSON files).
DbJsonSink = production DB + JSON beside source file.
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import db


class DbSink:

    def claim(self, file_seq_id, req, fmt, path, engine, batch_size):
        return db.claim_job(file_seq_id, req,fmt,path,engine, batch_size )

    def batch( self, job_id,file_seq_id,batch_no,first_row, records,meta):
        db.insert_batch(job_id,file_seq_id, batch_no, first_row, records, meta)

    def finish(self,job_id,status,total=0,batches=0,warnings=None, error=None):
        db.finish_job( job_id, status, total, batches,warnings,error )


class DbJsonSink(DbSink):
    """
    Stores parsed batches in Postgres and also creates one JSON
    beside the source PDF.

    Example:

        test.pdf
        test.json
    """

    def __init__(self, source_path: str):

        self.source_path = Path(source_path)

        # test.pdf -> test.json
        self.json_path = self.source_path.with_suffix(".json")

        # Temporary output while processing
        self.tmp_path = Path(
            str(self.json_path) + ".tmp"
        )

        self._fh = None
        self._first_record = True


    def claim(self,file_seq_id, req, fmt, path, engine, batch_size ):

        # Existing DB behavior
        job_id, should_process = super().claim( file_seq_id,req,fmt,path,engine,batch_size )

        if not should_process:
            return job_id, False

        # Make sure directory exists
        self.json_path.parent.mkdir( parents=True,exist_ok=True)

        # Remove previous incomplete temp output
        self.tmp_path.unlink( missing_ok=True)

        # Start writing JSON
        self._fh = self.tmp_path.open( "w",  encoding="utf-8" )

        self._fh.write("[\n")

        self._first_record = True

        return job_id, True


    def batch(self, job_id,file_seq_id,batch_no, first_row,records, meta):

        # ----------------------------------------------
        # Existing Postgres storage
        # ----------------------------------------------
        super().batch(job_id,file_seq_id,batch_no,first_row,records,meta)

        # ----------------------------------------------
        # JSON file
        # ----------------------------------------------
        if self._fh is None:
            return

        for record in records:

            clean_record = {}

            # Keep _row as first field
            if "_row" in record:
                clean_record["_row"] = record["_row"]

            # Then final parsed fields
            for key, value in record.items():

                if key in (  "_row","_raw","batch_id","sourcetype","bloc_recon_file_name","functionofmessage"):
                    continue

                clean_record[key] = value

            if not self._first_record:
                self._fh.write(",\n")

            json.dump(clean_record,self._fh,indent=2, ensure_ascii=False, default=str)

            self._first_record = False


    def finish( self,job_id, status,total=0,batches=0,warnings=None,error=None ):

        try:

            # ------------------------------------------
            # Existing DB completion
            # ------------------------------------------
            super().finish(job_id,status,total,batches,warnings,error)

            # ------------------------------------------
            # Close JSON
            # ------------------------------------------
            if self._fh is not None:

                self._fh.write("\n]\n")

                self._fh.close()

                self._fh = None


            if status == "COMPLETED":

                # Replace final JSON only after full success
                os.replace(self.tmp_path,self.json_path)

            else:

                self.tmp_path.unlink(missing_ok=True)

        except Exception:

            if self._fh is not None:

                try:
                    self._fh.close()
                except Exception:
                    pass

                self._fh = None

            self.tmp_path.unlink(missing_ok=True)

            raise


    def get_output_path(self) -> str:
        return str(self.json_path.resolve())


class FileSink:
    """
    Existing local test sink.

    Writes:
      batch_0001.json
      batch_0002.json
      _job.json
    """

    def __init__(self, out_dir: Path, clean: bool = True):

        self.out = Path(out_dir)

        if clean and self.out.exists():
            shutil.rmtree(self.out)

        self.out.mkdir(parents=True,exist_ok=True)

        self.job: dict[str, Any] = {}

        self.files: list[str] = []


    @staticmethod
    def _dump(path: Path, obj):

        path.write_text(json.dumps(obj,indent=2,default=str,ensure_ascii=False),encoding="utf-8")


    def claim(self,file_seq_id,req,fmt,path,engine,batch_size):

        self.job = {
            "file_seq_id": file_seq_id,
            "source_type": fmt,
            "file_path": path,
            "engine": engine,
            "batch_size": batch_size,
            "status": "PROCESSING",
            "started_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "request": req
        }

        return 0, True


    def batch(self,job_id,file_seq_id,batch_no,first_row,records,meta):

        name = f"batch_{batch_no:04d}.json"

        self._dump(self.out / name,
            {
                "file_seq_id": file_seq_id,
                "batch_no": batch_no,
                "record_count": len(records),
                "first_row": first_row,
                "payload": {
                    "records": records,
                    "meta": meta
                }
            }
        )

        self.files.append(name)


    def finish(self,job_id,status,total=0,batches=0,warnings=None,error=None):

        self.job.update(status=status,total_records=total,total_batches=batches,
            warnings=warnings or [],error=error,
            finished_at=datetime.now(timezone.utc).isoformat(),files=self.files )

        self._dump(self.out / "_job.json",self.job)