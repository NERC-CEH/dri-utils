import re
from datetime import datetime

from driutils.metadata_api.models.batch import Batch, BatchDataset
from driutils.metadata_api.utils import get_property

URI_ID_EXTRACT_REGEX = r".+\/([a-zA-Z0-9\-\_]+)$"


def transform_batches(raw_data: dict) -> list[Batch]:
    """Transform the raw API response into the desired format.

    Args:
        raw_data: Raw data from the API

    Returns:
        Transformed data with list of Batch objects
    """
    batches = []

    for item in raw_data["items"]:
        batch_id = get_property("identifier", item)
        datasets = []
        annotations = {}
        for data in item["hasPart"]:
            batch_dataset_id = get_property("@id", data)

            site = get_property("@id", get_property("originatingSite", data))
            if site:
                site = site.split("/")[-1].split("-")[-1]

            processing_level = get_property("@id", get_property("processingLevel", data))
            if processing_level:
                processing_level = processing_level.split("/")[-1]

            measure = get_property("@id", get_property("measure", data))
            variable, resolution, aggregation, units = [""] * 4
            if measure:
                variable, resolution, aggregation, units = measure.split("_")
                # Remove the id (e.g. http://fdri.ceh.ac.uk) from the variable section
                match = re.match(URI_ID_EXTRACT_REGEX, variable)
                variable = match.group(1) if match else variable
                # Reconstruct the measure now that the id prefix has been removed.
                measure = f"{variable}-{resolution}-{aggregation}-{units}"

            for annotation in data["hasAnnotation"]:
                annotation_name = re.search(r"#(.*)$", get_property("@id", annotation))
                if annotation_name:
                    annotations[annotation_name.group(1)] = get_property("value", get_property("hasValue", annotation))

            s3_key = data["sourceDataset"]
            s3_column = data["sourceColumnName"]
            s3_bucket = data["sourceBucket"]

            start_date = None
            end_date = None
            if data.get("temporal"):
                start_date = datetime.strptime(data["temporal"]["startDate"], "%Y-%m-%dT%H:%M:%S")
                end_date = datetime.strptime(data["temporal"]["endDate"], "%Y-%m-%dT%H:%M:%S")

            dataset = BatchDataset(
                batch_dataset_id=batch_dataset_id,
                variable=variable,
                units=units,
                aggregation=aggregation,
                filename=annotations["filename"],
                resolution=resolution,
                site=site,
                status=processing_level,
                measure=measure,
                dataset=annotations["dataset"],
                start_date=start_date,
                end_date=end_date,
                s3_key=s3_key,
                s3_column=s3_column,
                s3_bucket=s3_bucket,
            )
            datasets.append(dataset)

        batches.append(Batch(batch_id=batch_id, datasets=datasets))

    return batches
