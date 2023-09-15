import utils
import os
from statistics import mean
import pandas as pd


BLACKLISTED_FILES = [".DS_Store"]


def run_with_dataframe(
    name, output_dir, input_dir, func, include_dirs=False, subject="Symbol", **kwargs
):
    line = "-" * 85

    print(line)
    print(f"Running: {name}")
    print(line)

    if not os.path.exists(output_dir) and utils.should_save_files():
        os.makedirs(output_dir, exist_ok=True)

    files = utils.get_files_in_dir(input_dir, include_dirs)
    files = [i for i in files if i not in BLACKLISTED_FILES]

    # Using utils to be able to switch to different implementations as needed
    executor = utils.get_executor(workers=10)

    futures = []

    for file in files:
        futures.append(executor.submit(func, file))

    results = []
    for future in futures:
        try:
            res = future.result()
            results.append(res)
        except Exception as e:
            print(
                f"\nException occured while processing: {str(e)}\nStacktrace: {utils.get_formatted_traceback(e)}"
            )

    sorted_results = sorted(
        filter(lambda y: y["trades_taken"] > 0, results),
        key=lambda x: x["returns"],
        reverse=True,
    )

    top_5 = sorted_results[:5]
    top_5 = pd.DataFrame.from_dict(top_5)
    bottom_5 = sorted_results[-5:]
    bottom_5 = pd.DataFrame.from_dict(bottom_5)

    average_stock_growth = mean([x["stock_growth"] for x in results])
    average_returns = 0
    if len(results):
        average_returns = mean([x["returns"] for x in results])

    summary = {
        "average_returns": average_returns,
        "average_stock_growth": average_stock_growth,
    }

    print(line)

    print(
        f"SUMMARY ({name}) | Average Returns: {round(average_returns, 2)}% | Average Stock Growth: {round(average_stock_growth, 2)}%"
    )

    summary = pd.DataFrame.from_dict([summary])

    file_path = f"{output_dir}.xlsx"
    headers = [subject, "Returns (%)", "Stock Growth (%)", "Trades Taken"]

    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("Summary")
        writer.sheets["Summary"] = worksheet

        merge_format = workbook.add_format(
            {"border": 1, "align": "center", "valign": "vcenter"}
        )

        header_width = len(headers) - 1

        worksheet.merge_range(0, 0, 0, header_width, "Top 5", merge_format)

        top_5.to_excel(
            writer,
            sheet_name="Summary",
            startcol=0,
            startrow=1,
            index=False,
            header=headers,
        )

        worksheet.merge_range(8, 0, 8, header_width, "Bottom 5", merge_format)

        bottom_5.to_excel(
            writer,
            sheet_name="Summary",
            startcol=0,
            startrow=9,
            header=headers,
            index=False,
        )
        summary.to_excel(
            writer,
            sheet_name="Summary",
            startcol=0,
            startrow=16,
            index=False,
            header=["Average Returns (%)", "Market Growth (%)"],
        )

        df = pd.DataFrame.from_records(results)
        df = df.sort_values(df.columns[0])

        df.to_excel(
            writer,
            sheet_name="Stock Results",
            index=False,
            header=[subject, "Returns (%)", "Stock Growth (%)", "Trades Taken"],
        )
