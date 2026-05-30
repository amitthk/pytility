"""Agent 10 — PDF + Folder Export Agent (single-page PDF + HTML + artifacts)."""

import json
import re
from pathlib import Path

from schemas import GeneratedArtifacts


class PDFExportAgent:
    def export(self, state: dict) -> dict:
        if state.get("current_job_error"):
            return {}

        job = state["current_job"]
        job_id = job.get("job_id", "unknown")
        company = job.get("company", "unknown")
        job_title = job.get("job_title", "unknown")
        base_dir = state.get("output_base_dir", "./outputs")

        # Sanitize folder name
        folder_name = self._sanitize(f"{company}_{job_title}_{job_id}")
        output_dir = Path(base_dir) / folder_name
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"[PDF Export] Writing artifacts to: {output_dir}")

        # Save tailored HTML resume
        html_path = output_dir / "resume.html"
        tailored_html = state.get("current_tailored_html", "")
        if tailored_html:
            html_path.write_text(tailored_html, encoding="utf-8")
            print(f"[PDF Export] Saved resume.html")
        else:
            # Fallback to original CV
            tailored_html = state.get("cv_html_raw", "<html><body>No resume content</body></html>")
            html_path.write_text(tailored_html, encoding="utf-8")

        # Convert to single-page PDF
        pdf_path = output_dir / "resume.pdf"
        self._html_to_pdf(tailored_html, pdf_path)

        # Save cover letter
        cover_letter_path = output_dir / "cover_letter.md"
        cover_letter = state.get("current_cover_letter", "")
        if cover_letter:
            cover_letter_path.write_text(cover_letter, encoding="utf-8")
            print(f"[PDF Export] Saved cover_letter.md")

        # Save audit log
        audit_log_path = output_dir / "audit_log.json"
        audit_log = state.get("current_audit_log", [])
        audit_log_path.write_text(json.dumps(audit_log, indent=2), encoding="utf-8")
        print(f"[PDF Export] Saved audit_log.json ({len(audit_log)} entries)")

        # Save alignment matrix for reference
        alignment = state.get("current_alignment", {})
        if alignment:
            alignment_path = output_dir / "alignment_matrix.json"
            alignment_path.write_text(json.dumps(alignment, indent=2), encoding="utf-8")

        artifacts = GeneratedArtifacts(
            job_id=job_id,
            output_folder=str(output_dir),
            html_resume_path=str(html_path),
            pdf_resume_path=str(pdf_path),
            cover_letter_path=str(cover_letter_path),
            audit_log_path=str(audit_log_path),
        )

        print(f"[PDF Export] All artifacts saved for: {company} - {job_title}")

        return {
            "completed_artifacts": [artifacts.model_dump()],
            "log_messages": [f"Exported artifacts for {job_id} to {output_dir}"],
        }

    def _html_to_pdf(self, html_content: str, pdf_path: Path):
        """Convert HTML to a single-page (poster-style) PDF using WeasyPrint."""
        try:
            from weasyprint import HTML, CSS

            # CSS for single continuous page
            single_page_css = CSS(string="""
                @page {
                    size: 210mm 2000mm;
                    margin: 10mm;
                }
                * {
                    page-break-before: avoid !important;
                    page-break-after: avoid !important;
                    page-break-inside: avoid !important;
                }
            """)

            html_doc = HTML(string=html_content)
            html_doc.write_pdf(str(pdf_path), stylesheets=[single_page_css])
            print(f"[PDF Export] Generated single-page PDF: {pdf_path}")

        except ImportError:
            print("[PDF Export] WeasyPrint not available, skipping PDF generation")
            pdf_path.write_text("PDF generation requires WeasyPrint with system dependencies (cairo, pango)")
        except Exception as e:
            print(f"[PDF Export] PDF generation failed: {e}")
            pdf_path.write_text(f"PDF generation failed: {e}")

    def _sanitize(self, name: str) -> str:
        """Sanitize a string for use as a filesystem path component."""
        name = re.sub(r'[^\w\s-]', '', name)
        name = re.sub(r'[\s]+', '_', name)
        return name[:100]
