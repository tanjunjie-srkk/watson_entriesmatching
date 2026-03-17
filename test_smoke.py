"""Quick smoke test for pipeline + Excel export."""
from pathlib import Path
from reconciliation import run_reconciliation_from_paths, export_to_excel

root = Path(r"c:\Users\TanJunJie\OneDrive - SRKK Group\Project\watson_entriesmatching\OneDrive_2026-03-09\Shopee Sample Reports (Testing)\scenario2")
xlsx = sorted(root.rglob("*.xlsx"))
inc = [str(p) for p in xlsx if p.name.startswith("Income.released")]
bal = [str(p) for p in xlsx if p.name.startswith("my_balance_transaction")]
sales = [str(p) for p in xlsx if p.name.startswith("SalesReport")]
print(f"Files: income={len(inc)}, balance={len(bal)}, sales={len(sales)}")

result = run_reconciliation_from_paths(inc, bal, sales, progress_callback=lambda m: print(m))
print("Timings:", result["timings"])
print("Stats keys:", list(result["stats"].keys()))
print("Report rows:", result["report"].height)
print("Outs+Refund rows:", result["outstanding_with_refund"].height)

xl = export_to_excel({
    "report": result["report"],
    "recon_report": result["recon_report"],
    "Outstanding": result["outstanding"],
    "Refund": result["refund"],
    "outstanding_with_refund": result["outstanding_with_refund"],
    "income_not_balance": result["income_not_balance"],
    "balance_not_income": result["balance_not_income"],
})
print(f"Excel size: {len(xl.getvalue())} bytes")
print("ALL TESTS PASSED")
