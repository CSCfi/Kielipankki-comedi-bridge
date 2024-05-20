import requests

import click
import lxml
from sickle import Sickle, oaiexceptions


class ParseError(Exception):
    """
    For reporting errors in XML parsing in a more user-friendly way
    """


class UploadError(Exception):
    """
    For reporting errors in COMEDI uploads
    """


def metashare_cmdi_records(metashare_api_url):
    """
    Iterate over all records in META-SHARE in CMDI format.
    """
    sickle = Sickle(metashare_api_url)
    metadata_records = sickle.ListIdentifiers(
        **{
            "metadataPrefix": "info",
            "ignore_deleted": True,
        }
    )
    for record in metadata_records:
        for metadata_format in ["cmdi0554", "cmdi0571", "cmdi2312", "cmdi9836"]:
            try:
                response = sickle.GetRecord(
                    identifier=record.identifier, metadataPrefix=metadata_format
                )
            except oaiexceptions.CannotDisseminateFormat:
                continue

            yield response.xml
            break


def upload_cmdi_to_comedi(cmdi_data, urn, comedi_upload_url, session_id, published):
    """
    Upload the given XML record to COMEDI

    In case of errors, a message is printed to stdout and the problematic record is
    skipped.
    """

    params = {
        "group": "FIN-CLARIN",
        "session-id": session_id,
    }
    if published:
        params["published"] = True

    response = requests.post(
        comedi_upload_url,
        params=params,
        files={"file": (f"{urn}.xml", cmdi_data, "text/xml")},
    )
    response.raise_for_status()

    if "error" in response.json():
        raise UploadError(f"Upload failed: {response.json()['error']}")
    if "success" not in response.json() or not response.json()["success"]:
        raise UploadError("Something went wrong: {response.json()}")


def extract_cmdi_metadata(metashare_record, modifiers):
    """
    Return the CMDI metadata from META-SHARE record as XML string.

    Modifiers are assumed to be functions that take a single parameter (the CMDI record
    XML as lxml) and modify it in place.
    """
    if not modifiers:
        modifiers = []

    try:
        cmdi_record = metashare_record.xpath(
            "oai:metadata/cmd:CMD",
            namespaces={
                "oai": "http://www.openarchives.org/OAI/2.0/",
                "cmd": "http://www.clarin.eu/cmd/",
            },
        )[0]
    except IndexError:
        raise ParseError("No CMDI record found")

    for modifier in modifiers:
        modifier(cmdi_record)

    xml_content = lxml.etree.tostring(
        cmdi_record, xml_declaration=True, encoding="utf-8"
    )
    return xml_content


def self_link_urn_only(metashare_cmdi_record):
    """
    Adjust the self identifier to be a plain URN, without the URL part. If self link is
    not present (e.g. record creation in progress).

    E.g. "http://urn.fi/urn:nbn:fi:lb-123" becomes "urn:nbn:fi:lb-123".

    """
    try:
        self_link_element = metashare_cmdi_record.xpath(
            "cmd:Header/cmd:MdSelfLink", namespaces={"cmd": "http://www.clarin.eu/cmd/"}
        )[0]
    except IndexError as err:
        raise ParseError("Self link not found")
    self_link_element.text = self_link_element.text.split("urn.fi/")[1]


def extract_urn(metashare_record):
    """
    Return the unique part of the URN (e.g. "lb-1234") from META-SHARE record.

    Unfortunately some of our URNs have trailing whitespace (e.g. "lb-1234 "): that is
    not allowed in COMEDI identifiers, so whitespace is stripped.
    """
    try:
        urn_url = metashare_record.xpath(
            "oai:metadata/cmd:CMD/cmd:Components/cmd:resourceInfo/cmd:identificationInfo/cmd:identifier/text()",
            namespaces={
                "oai": "http://www.openarchives.org/OAI/2.0/",
                "cmd": "http://www.clarin.eu/cmd/",
            },
        )[0]
    except IndexError:
        raise ParseError("No urn found")

    try:
        urn = urn_url.split("urn:nbn:fi:")[1]
    except IndexError:
        raise ParseError(f"Could not parse urn {urn_url}")

    return urn.strip()


def delete_from_comedi(identifier, session_id):
    """
    Delete a record from COMEDI.
    """
    requests.get(
        "https://clarino.uib.no/comedi/rest?command=delete-record",
        params={"session-id": session_id, "identifier": identifier},
    )


@click.command()
@click.argument("comedi_session_id")
@click.option("--metashare-api-url", default="https://kielipankki.fi/md_api/que")
@click.option("--comedi-upload-url", default="https://clarino.uib.no/comedi/upload")
@click.option("--publish/--unpublish", default=False)
@click.option("--delete-before-upload", default=False, is_flag=True)
def send_metadata(
    comedi_session_id,
    metashare_api_url,
    comedi_upload_url,
    publish,
    delete_before_upload,
):
    """
    Send all metadata from META-SHARE to COMEDI.
    """
    successful_records = 0
    failed_records = 0
    for cmdi_record in metashare_cmdi_records(metashare_api_url):
        metashare_identifier = cmdi_record.xpath(
            "oai:header/oai:identifier/text()",
            namespaces={"oai": "http://www.openarchives.org/OAI/2.0/"},
        )[0]

        try:
            cmdi_data = extract_cmdi_metadata(
                cmdi_record, modifiers=[self_link_urn_only]
            )

            urn = extract_urn(cmdi_record)
        except ParseError as err:
            click.echo(
                f"Error when handling META-SHARE record {metashare_identifier}: {str(err)}",
                err=True,
            )
            failed_records += 1
            continue

        if delete_before_upload:
            delete_from_comedi(urn, comedi_session_id)

        try:
            upload_cmdi_to_comedi(
                cmdi_data,
                urn,
                comedi_upload_url,
                session_id=comedi_session_id,
                published=publish,
            )
        except UploadError as err:
            click.echo(
                f"COMEDI upload failed for META-SHARE record {metashare_identifier} / {urn}: {str(err)}",
                err=True,
            )
            failed_records += 1
        else:
            click.echo(f"Successfully uploaded {metashare_identifier} / {urn}")
            successful_records += 1

    print(
        f"{successful_records + failed_records} processed, "
        f"{successful_records} uploads and {failed_records} failures."
    )


if __name__ == "__main__":
    # pylint does not understand click wrappers
    # pylint: disable=no-value-for-parameter
    send_metadata()
