# !/usr/bin/env python3

"""
Use at your own risk.  No compatibility or maintenance or other assurance of suitability is expressed or implied.
Update or modify as needed
"""

# 4.x notes. Trying new approach.  Fetch zip and unpack various object elements.
# rather than use export/import I'll use the base APIs because they support /api/apollo/apps/[appname]/[objecttype]
# endpoints because that will (hopefully) simplify linking things together
#
# assumptions
# 1. only one app will be imported or exported at a time
# 2. signals and signals_aggr collections will be skipped
# 3. only collections in the 'default' cluster will be processed
#
# initially supported object types
# App
# collection
# pipelines
# profiles
# datasources
#
# support added for
# search clusters
# query rewrite

# Types with UNKNOWN affect
# features
# links
# objectGroups

#
# query Fusion for all datasources, index pipelines and query pipelines.  Then make lists of names
# which start with the $PREFIX so that they can all be exported.

#  Requires a python 3.x+ interpreter (tested on 3.8.9)
import textwrap
import json
import sys
import argparse
import os
import datetime
import re
import requests
from typing import Literal, Sequence
from requests import Response
from io import BytesIO
from zipfile import ZipFile
from argparse import RawTextHelpFormatter

try:
    # get current dir of this script
    cwd = os.path.dirname(os.path.realpath(sys.argv[0]))

    OBJ_TYPES = {
        "fusionApps": {"ext": "APP"},
        "zones": {"ext": "ZN"},
        "templates": {"ext": "TPL"},
        "dataModels": {"ext": "DM"},
        "indexPipelines": {"ext": "IPL"},
        "queryPipelines": {"ext": "QPL"},
        "indexProfiles": {"ext": "IPF"},
        "queryProfiles": {"ext": "QPF"},
        "parsers": {"ext": "PS"},
        "dataSources": {"ext": "DS"},
        "collections": {"ext": "COL", "urlType": "collection"},
        "jobs": {"ext": "JOB"},
        "tasks": {"ext": "TSK"},
        "sparkJobs": {"ext": "SPRK", "filelist": []},
        "blobs": {"ext": "BLOB", "urlType": "blob"},
        "features": {"ext": "CF"},
        # 👆 features can't be fetched by id in the export API but come along with the collections.
    }

    search_clusters = {}
    collections = []
    PARAM_SIZE_LIMIT = 6400
    TAG_SUFFIX: str = "_mergeForm"

    def eprint(*args, **kwargs) -> None:
        print(*args, file=sys.stderr, **kwargs)

    def sprint(msg) -> None:
        # change inputs to *args, **kwargs in python 3
        # print(*args, file=sys.stderr, **kwargs)
        print(msg)
        sys.stdout.flush()

    def debug(msg) -> None:
        if args.debug:
            sprint(msg)

    def verbose(msg) -> None:
        if args.verbose:
            sprint(msg)

    def get_suffix(obj_type) -> str:
        return f"_{OBJ_TYPES[obj_type]['ext']}.json"

    def apply_suffix(f, suffix_type) -> str:
        suf = OBJ_TYPES[suffix_type]["ext"]
        if not f.endswith(suf):
            return f"{f}{get_suffix(suffix_type)}"
        return f"{f}.json"

    def init_args() -> None:
        env = {}  # some day we may get better environment passing
        debug("initArgs start")

        # setting come from command line but if not set then pull from environment
        if args.server is None:
            args.server = init_args_from_maps("lw_IN_URL", "https://localhost", os.environ, env)
        elif not args.server.lower().startswith("http"):
            eprint("--server argument must be in url format e.g. http://lowercase")
            sys.exit(2)

        if args.user is None:
            args.user = init_args_from_maps("lw_USERNAME", "admin", os.environ, env)

        if args.password is None:
            args.password = init_args_from_maps("lw_PASSWORD", "password123", os.environ, env)

        if args.app is None and args.zip is None:
            sys.exit("either the --app or the --zip argument is required.  Can not proceed.")

        if args.dir is None and args.zip is None:
            # make default dir name
            def_dir = f"{str(args.app)}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}"
            args.dir = def_dir
        elif args.dir is None and args.zip is not None:
            def_dir = f"{str(args.zip)}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}"
            args.dir = def_dir

    def init_args_from_maps(
        key: Literal["lw_USERNAME", "lw_PASSWORD", "lw_IN_URL"], default: str, penv: dict, env: dict
    ) -> str:
        if key in penv:
            debug(f"penv has_key {key}:{penv[key]}")
            return penv[key]
        else:
            if key in env:
                debug(f"env has_key {key}:{env[key]}")
                return env[key]
            else:
                debug(f"default setting for {key}:{default}")
                return default

    def make_base_uri() -> str:
        uri = args.server + "/api"
        return uri

    def do_http(
        url: str,
        usr: str = None,
        pswd: str = None,
        headers: dict = {},
        params: dict = {},
    ) -> Response:
        response = None
        auth = None
        if usr is None:
            usr = args.user
        if pswd is None:
            pswd = args.password

        if args.jwt is not None:
            headers["Authorization"] = f"Bearer {args.jwt}"
        else:
            auth = requests.auth.HTTPBasicAuth(usr, pswd)

        verify = not args.noVerify
        try:
            debug(f"calling requests.get url:{url} usr:{usr} pswd:{pswd} headers:{str(headers)}")
            response = requests.get(url, auth=auth, headers=headers, verify=verify, params=params)
            return response
        except requests.ConnectionError as err:
            eprint(err)

    def make_export_params_from_json(j: dict) -> Sequence[dict]:
        """
        Make n export URL containing all the needed id えぇめんts existing in the objects.json file
        :param j:
        :return　list of sized dictionaries of params such that none exceeds 6.5k:
        """
        # url = make_base_uri() + "/objects/export"
        all_params = []
        params = {"filterPolicy": "system", "deep": "false"}
        # all_params.insert(0,params)

        if j is None or "objects" not in j:
            return all_params

        objects = j["objects"]
        # save the fusionApps[0] element
        if "fusionApps" in objects and isinstance(objects["fusionApps"], list):
            OBJ_TYPES["fusionApps"]["appDef"] = objects["fusionApps"][0]

        keys = OBJ_TYPES.keys()

        for key in keys:
            if key in objects:
                items = objects[key]

                itms = []
                for item in items:
                    if (
                        isinstance(item, dict)
                        and "id" in item
                        and not (key == "blobs" and item["id"].startswith("prefs-"))
                    ):
                        itms.append(item["id"])

                if isinstance(itms, list) and "urlType" in OBJ_TYPES[key]:
                    # check and see if adding this set of params would push us over the limit.
                    # If so, store off the smaller params set in allParams and start a new one
                    if len(str(params)) + len(str(itms)) > PARAM_SIZE_LIMIT:
                        all_params.insert(0, params)
                        params = {
                            "filterPolicy": "system",
                            "deep": "false",
                            OBJ_TYPES[key]["urlType"] + ".ids": itms,
                        }
                    # otherwise add to the current params list since we are under the size limit
                    else:
                        params[OBJ_TYPES[key]["urlType"] + ".ids"] = itms

        # add any fragment params in to the results.  For small apps, this may be everything
        if len(params) > 1 and params not in all_params:
            all_params.insert(0, params)
        return all_params

    def do_get_json_app() -> None:
        """
        fetch a json export of the app.  The purpose is to get a list of everything that belongs to the App.
        Then, after extracting all but the blobs and collections fetch those into Zip(s) and extract including files.
        All this to work around the fact that we can't export an app to a zip without query_rewrite
        """
        url = f"{make_base_uri()}/objects/export?filterPolicy=system&app.ids={args.app}"
        headers = {"accept": "application/json"}
        try:
            debug(f"calling requests.get url:{url}  headers:{str(headers)}")
            verbose(f"Getting JSON elements of APP {args.app} from {args.server}")
            response: Response = do_http(url=url, headers=headers)
            if response is not None and response.status_code == 200:
                content_type = response.headers["Content-Type"]
                if "application/json" in content_type:
                    j = json.loads(response.content)
                    export_params = make_export_params_from_json(j)
                    # In addition to processing all the zip files from exportParam sets, we need to output
                    # APP and possibly features but don't grab blobs and collections.  Those need to come from full zips
                    # Might depend on 5.x release whether the object exists - remove them if present
                    if "blobs" in j["objects"]:
                        j["objects"].pop("blobs")
                    else:
                        debug("blobs key does not exist")
                    if "collections" in j["objects"]:
                        j["objects"].pop("collections")
                    else:
                        debug("collections key does not exist")

                    verbose(f"Extracting contents of downloaded APP {args.app}")
                    extract_app_from_zip(objects=j, validate_app_name=True)
                    url = make_base_uri() + "/objects/export"
                    index = 0
                    for params in export_params:
                        if args.verbose:
                            index += 1
                            sprint(f"\nFetching Zip export from {url} params set {index}")
                        zipfile = do_http_zip_get(url, params=params)
                        extract_app_from_zip(zipfile, validate_app_name=False)
            else:
                if response is not None and response.status_code == 401 and "unauthorized" in response.text:
                    eprint(f"Non OK response of {str(response.status_code)} for URL: {url} \nCheck your password\n")
                elif response is not None and response.status_code:
                    eprint(f"Non OK response of {str(response.status_code)} for URL: {url}")
        except Exception as err:
            eprint(f"Exception when fetching App: {str(err)}")

    def do_http_zip_get(url: str, usr: str = None, pswd: str = None, params: dict = {}) -> ZipFile:
        response = None
        response = do_http(url, usr, pswd, params=params)
        if response is not None and response.status_code == 200:
            content_type = response.headers["Content-Type"]
            debug("content_type of response is " + content_type)
            # use a contains check since the content_type may be 'application/json; utf-8' or multi-valued
            if "application/zip" in content_type:
                content = response.content
                zipfile = ZipFile(BytesIO(content))
                return zipfile
            else:
                eprint(f"Non Zip content type of '{content_type}' for url:'{url}'")
        elif response is not None and response.status_code != 200:
            eprint(f"Non OK response of {str(response.status_code)} for URL: {url}")
            if response.reason is not None:
                eprint(f"\tReported Reason: '{response.reason}'")
        else:
            # Bad url?? bad protocol?
            eprint(f"Problem requesting URL: '{url}'. Check server, protocol, port, etc.")

    def extract_app_from_zip(zipfile: ZipFile = None, objects: dict = None, validate_app_name: bool = True) -> None:
        """
        Either zipfile or objects must be valued but not both

        :param zipfile:
        :param objects:
        :param validate_app_name: if True, the objects.fusionApps[0].id must equal args.app or an error will be printed
        :return:
        """

        if (zipfile and objects) or not (zipfile or objects):
            raise ValueError("Either zipfile or objects parameter is required.")

        filelist = []
        if zipfile:
            filelist = zipfile.namelist()
            if "objects.json" not in filelist:
                sys.exit("Exported zip does not contain objects.json.  Can not proceed.")
            jstr = zipfile.open("objects.json").read()
            objects = json.loads(jstr)

        # check to be sure that the requested application exists and give error if not
        if (
            validate_app_name
            and args.app is not None
            and not (
                objects
                and (len(objects["objects"]) > 0)
                and (objects["objects"]["fusionApps"])
                and (objects["objects"]["fusionApps"][0]["id"])
                and (objects["objects"]["fusionApps"][0]["id"] == args.app)
            )
        ):
            sys.exit(f"No Fusion App called '{args.app}' found on server '{args.server}'. Can not proceed.")

        # sorting ensures that collections are known when other elements are extracted
        for obj_type in sorted(objects["objects"].keys()):
            # obj will be the name of the object type just under objects
            # i.e. objects.collections, indexPipelines etc.
            do_object_type_switch(objects["objects"][obj_type], obj_type)

        # global collections[] will hold exported collection names.
        # Get the configsets for those and write them out as well
        for filename in filelist:
            if should_extract_file(filename):
                extract_from_zipfile(filename, zipfile)
            elif should_extract_embedded_zipfile(filename):
                extract_zipfile(filename, zipfile)

        if zipfile:
            zipfile.close()

    def should_extract_file(filename: str) -> bool:
        """Check for blob zips which should be extracted intact or non-zipped configsets."""
        path = filename.split("/")
        extension = os.path.splitext(path[-1])
        file = extension[0]
        ext = extension[-1]
        if path[0] == "blobs":
            return not file.startswith("prefs-")
        # in 4.0.2 configsets are already unzip so each file can be extracted.
        # this block should catch 4.0.2 case
        # and shouldExtractConfig will catch the 4.0.1 case
        elif len(path) > 2 and path[0] == "configsets" and ext != ".zip" and path[1] in collections:
            return True
        return False

    def should_extract_embedded_zipfile(filename: str) -> bool:
        """Check for embedded and zipped configsets in need of extraction."""
        path = filename.split("/")
        extension = os.path.splitext(path[-1])
        file = extension[0]
        ext = extension[-1]
        if path[0] == "configsets" and ext == ".zip" and file in collections:
            return True
        return False

    def extract_from_zipfile(filename: str, zipfile: ZipFile) -> None:
        """
        There seems to be a bug in the creation of the zip by the export routine and some files are zero length.
        Don't save these since they would produce an empty file which would overwrite the blob on import.
        """
        if zipfile.getinfo(filename).file_size > 0:
            zipfile.extract(filename, args.dir)
        else:
            eprint(f"File '{filename}' in archive is zero length. Extraction skipped.")

    def extract_zipfile(filename: str, zipfile: ZipFile):
        path = filename.split("/")
        path[-1] = os.path.splitext(path[-1])[0]
        output_dir = os.path.join(args.dir, *path)
        zipfile_data = BytesIO(zipfile.read(filename))
        with ZipFile(zipfile_data) as zf:
            zf.extractall(output_dir)

    def do_object_type_switch(elements, obj_type):
        """Do something with some json based on the type of the json array."""
        switcher = {
            "fusionApps": collect_by_id,
            "collections": lambda l_elements, l_type: collect_collections(l_elements, l_type),
            "features": lambda l_elements, l_type: collect_features(l_elements, l_type),
            "indexPipelines": collect_by_id,
            "queryPipelines": collect_by_id,
            "indexProfiles": collect_profile_by_id,
            "queryProfiles": collect_profile_by_id,
            "parsers": collect_by_id,
            "dataSources": collect_by_id,
            "tasks": collect_by_id,
            "jobs": collect_by_id,
            "sparkJobs": collect_by_id,
            "templates": lambda l_elements, l_type: collect_by_id(l_elements, l_type, "id", "name"),
            "zones": lambda l_elements, l_type: collect_by_id(l_elements, l_type, "id", "name"),
            "dataModels": collect_by_id,
            "blobs": lambda l_elements, l_type: collect_by_id(l_elements, l_type, "filename", "dir"),
        }
        # get the function matchng the type or a noop
        process_typed_element_func = switcher.get(obj_type, lambda *args: None)
        # call the function passing elements and type
        process_typed_element_func(elements, obj_type)

    def sorted_deep(d):
        """Recurse through the entire JSON tree and sort all key/values."""

        def make_tuple(v):
            return (*v,) if isinstance(v, (list, dict)) else (v,)

        if isinstance(d, list):
            return sorted(map(sorted_deep, d), key=make_tuple)
        if isinstance(d, dict):
            return {k: sorted_deep(d[k]) for k in sorted(d)}
        return d

    def json_to_file(j_data, obj_type, filename, alt_sub_dir=None) -> None:
        j_data = sorted_deep(j_data)
        # replace spaces in filename to make the files sed friendly
        filename2 = filename.replace(" ", "_")
        if alt_sub_dir is None:
            sub_dir = obj_type
        else:
            sub_dir = alt_sub_dir

        if obj_type and not os.path.isdir(os.path.join(args.dir, sub_dir)):
            os.makedirs(os.path.join(args.dir, sub_dir))

        with open(os.path.join(args.dir, sub_dir, filename2), "w") as outfile:
            # sorting keys makes the output source-control friendly.  Do we also want to strip out
            if "updates" in j_data:
                j_data.pop("updates", None)
            if "modifiedTime" in j_data:
                j_data.pop("modifiedTime", None)
            if "version" in j_data:
                j_data.pop("version", None)

            if not args.noStageIdMunge and "stages" in j_data:
                stages = j_data["stages"]
                for i, stage in enumerate(stages):
                    if "secretSourceStageId" in stage:
                        stage.pop("secretSourceStageId", None)
                    stage["id"] = munge_stage_id(stage, str(i))

            outfile.write(json.dumps(j_data, indent=4, sort_keys=True, separators=(", ", ": ")))
            outfile.close()

    def munge_stage_id(stage, idx_str) -> str:
        stage_type = stage.get("type", "")
        label = stage.get("label", "")
        return re.sub("[ -]", "_", f"{stage_type}:{label}:{idx_str}")

    def make_script_readable(element: object, tag: str) -> None:
        if tag in element:
            script = element[tag]
            # update element with split script
            element[tag + TAG_SUFFIX] = script.splitlines()

    def make_diff_friendly(e, obj_type) -> None:
        xform_tags = [
            "script",  # scala script, python
            "transformScala",  # PBL
            "transformSQL",  # PBL
            "sql",  # sqlTemplate
            "sparkSQL",  # sqlTemplate, headTail, tokenPhraseSpellCorrection
            "misspellingSQL",  # synonym detection
            "phraseSQL",  # synonym detection
            "rollupSql"  # sql_template
            # ,"analyzerConfigQuery" # candidate json from some 4 sisters
            # ,"notes" # candidate for multi-line descript/notes field
        ]

        if isinstance(e, dict) and obj_type.endswith("Pipelines") and ("stages" in e.keys()):
            for stage in e["stages"]:
                if isinstance(stage, dict) and ("script" in stage.keys()):
                    stg_keys = stage.keys()
                    if "script" in stg_keys:
                        make_script_readable(stage, "script")
                    if "condition" in stg_keys and "\n" in stage["condition"]:
                        make_script_readable(stage, "condition")
        elif isinstance(e, dict) and obj_type == "sparkJobs":
            for tag in xform_tags:
                if tag in e:
                    make_script_readable(e, tag)

    def collect_by_id(elements, obj_type, key_field="id", name_space_field=None):
        for ele in elements:
            if key_field not in ele and "resource" in ele:
                key_field = "resource"
            ele_id = ele[key_field]
            if obj_type == "blobs" and ele_id.startswith("prefs-"):
                continue
            verbose("Processing '" + obj_type + "' object: " + ele_id)
            # spin thru e and look for 'stages' with 'script'
            make_diff_friendly(ele, obj_type)

            # some jobs have : in the id, some blobs have a path.  Remove problem characters in filename
            if name_space_field is not None and name_space_field in ele:
                ns = ele[name_space_field]
                if ns is not None:
                    ns = re.sub(r"^[/\\:\s]", "", ns)
                    filename = apply_suffix(
                        re.sub(r"[/\\:.\s]", "_", ns) + "_" + ele_id.replace(":", "_").replace("/", "_"),
                        obj_type,
                    )
            else:
                filename = apply_suffix(ele_id.replace(":", "_").replace("/", "_"), obj_type)
            json_to_file(j_data=ele, obj_type=obj_type, filename=filename)

    def collect_profile_by_id(elements, obj_type) -> None:
        """
        This code is tentative.
        The pipeline elements contains a sub object called 'ALL' which then contains the list we want
        Update: looks like 4.1 gets rid of the ALL
        """
        mylist = []
        if isinstance(elements, dict) and ("ALL" in elements.keys()):
            mylist = elements["ALL"]
        # 4.0.0 seems to give a dict of items id:[{id:val...}]
        elif isinstance(elements, dict):
            mylist = []
            for k in elements:
                v = elements[k]
                if isinstance(v, dict):
                    mylist.append(v)
                elif isinstance(v, list):
                    mylist.extend(v)
        elif isinstance(elements, list):
            mylist = elements
        if mylist is not None:
            collect_by_id(mylist, obj_type)
        elif len(elements) > 1:
            eprint(
                "Unknown JSON structure encountered.  Profiles subtree should be 'ALL' element or array of Profiles."
            )

    def collect_features(elements, obj_type="features") -> None:
        for col in elements:
            features = elements[col]
            filename = apply_suffix(col.replace(":", "_").replace("/", "_"), obj_type)
            json_to_file(
                j_data=features,
                obj_type=obj_type,
                filename=filename,
                alt_sub_dir="collectionFeatures",
            )

    def collect_collections(elements, obj_type="collections") -> None:
        keep = []
        for ele in elements:
            ele_id = ele["id"]
            keep.append(ele)
            # make sure associated clusters are exported
            # keep track of the default collections (global) we are exporting so that schema can be exported as well
            # do not export schema for collections on non-default clusters.  Best to not mess with remote config
            if ele["searchClusterId"] == "default":
                collections.append(ele_id)

        collect_by_id(keep, obj_type)

    def collect_index_pipelines(elements) -> None:
        collect_by_id(elements, "indexPipelines")

    def main() -> None:
        init_args()
        # create if missing
        if not os.path.isdir(args.dir):
            os.makedirs(args.dir)
        # Fetch solr clusters map so we can export if needed
        # target = args.app
        zipfile = None
        if args.zip is not None:
            sprint(f"Getting export zip from file '{args.zip}'")
            zipfile = ZipFile(args.zip, "r")
            extract_app_from_zip(zipfile)
        else:
            do_get_json_app()

    if __name__ == "__main__":
        scriptName = os.path.basename(__file__)
        parser = argparse.ArgumentParser(
            description=textwrap.indent(
                textwrap.dedent(
                    """\
           +-----------------------------------------------------------------------------------------+
           | - Get artifacts associated with a Fusion app.                                           |
           | - Store them together in a folder as sub-folders and flat files.                        |
           | - These files can be stored, manipulate and uploaded to a Fusion instance as needed.    |
           | - NOTE: if launching from get_fusion_app.sh, defaults will be pulled from the bash environment, | 
           |   plus values from bin/lw_env.sh                                                        |
           +-----------------------------------------------------------------------------------------+
        """
                ),
                "\t",
                lambda line: True,
            ),
            formatter_class=RawTextHelpFormatter,
        )

        # parser.add_argument_group(
        #   'bla bla bla instruction go here and they are really long \t and \n have tabs and\n newlines'
        # )
        parser.add_argument(
            "-a",
            "--app",
            type=str,
            help="App to export",
            default="tf",
        )
        parser.add_argument("--dir", type=str, help="Output directory, default: '${app}_ccyymmddhhmm'.")
        parser.add_argument(
            "-s",
            "--server",
            metavar="SVR",
            type=str,
            help=textwrap.dedent(
                """\
            Server url (see below).
            Default: 
            - ${lw_IN_SERVER} or 'https://fusion5.tfcom-cluster-na-srchdev.clouddev.thermofisher.net'.
            Other Values: 
            - 'https://fusion5.tfcom-cluster-na-srchqa1.clouddqa.thermofisher.net'
            - 'https://fusion5.tfcom-cluster-na-srchprod.cloud.thermofisher.net'
            """
            ),
            default="https://fusion5.tfcom-cluster-na-srchdev.clouddev.thermofisher.net",
        )
        parser.add_argument(
            "-u",
            "--user",
            type=str,
            help="Fusion user name, default: ${lw_USER} or 'admin'.",
            default="admin1",
        )
        parser.add_argument(
            "--password",
            type=str,
            help="Fusion Password,  default: ${lw_PASSWORD} or 'password123'.",
            default="password123",
        )
        parser.add_argument(
            "--jwt",
            help="JWT token for access to Fusion.  If set, --user and --password will be ignored. Default: None",
            # type=str,
            default=None,
        )
        parser.add_argument(
            "-v",
            "--verbose",
            help="Print details, default: False",
            action="store_true",
        )
        parser.add_argument(
            "-d",
            "--debug",
            help="Print debug messages while running, default: False",
            action="store_true",
        )
        parser.add_argument(
            "--noVerify",
            help="Do not verify SSL certificates if using https, default: False",
            action="store_true",
        )
        parser.add_argument(
            "-z",
            "--zip",
            help="Path and name of the Zip file to read from rather than using an export from --server, default: None",
            type=str,
            default=None,
        )
        parser.add_argument(
            "--noStageIdMunge",
            help="Experimental: may become default.  If True, do not munge pipeline stage ids. default: False",
            action="store_true",
        )

        # print("args: " + str(sys.argv))
        args = parser.parse_args()
        main()
except Exception as e:
    msg = None
    if hasattr(e, "msg"):
        msg = e.msg
    elif hasattr(e, "text"):
        msg = e["text"]
    else:
        msg = str(e)
    print(f"Exception: {msg}", file=sys.stderr)
