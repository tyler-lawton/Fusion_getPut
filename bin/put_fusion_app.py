#!/usr/bin/env python3
"""
Use at your own risk.  No compatibility or maintenance or other assurance of suitability is expressed or implied.
Update or modify as needed
"""

#
# query Fusion for all datasources, index pipelines and query pipelines.  Then make lists of names
# which start with the $PREFIX so that they can all be exported.

#  Requires a python 2.7.5+ interpreter
try:
    import json, sys, argparse, os, subprocess, sys, requests, datetime, re, urllib
    from argparse import RawTextHelpFormatter
    from pathlib import Path

    # get current dir of this script
    cwd: str = os.path.dirname(os.path.realpath(sys.argv[0]))

    app_name = None

    fusion_version = "4.0.0"

    # this still leaves features, objectGroups, links ignored from the objects.json export
    OBJ_TYPES = {
        "fusionApps": {"ext": "APP", "filelist": []},
        "zones": {
            "ext": "ZN",
            "filelist": [],
            "api": "templating/zones",
            "linkType": "zone",
        },
        "templates": {
            "ext": "TPL",
            "filelist": [],
            "api": "templating/templates",
            "linkType": "template",
        },
        "dataModels": {
            "ext": "DM",
            "api": "data-models",
            "filelist": [],
            "linkType": "data-model",
        },
        "indexPipelines": {
            "ext": "IPL",
            "filelist": [],
            "api": "index-pipelines",
            "linkType": "index-pipeline",
        },
        "queryPipelines": {
            "ext": "QPL",
            "filelist": [],
            "api": "query-pipelines",
            "linkType": "query-pipeline",
        },
        "indexProfiles": {
            "ext": "IPF",
            "filelist": [],
            "api": "index-profiles",
            "linkType": "index-profile",
        },
        "queryProfiles": {
            "ext": "QPF",
            "filelist": [],
            "api": "query-profiles",
            "linkType": "query-profile",
        },
        "parsers": {"ext": "PS", "filelist": [], "linkType": "parser"},
        "dataSources": {
            "ext": "DS",
            "api": "connectors/datasources",
            "filelist": [],
            "substitute": True,
            "linkType": "datasource",
        },
        "collections": {"ext": "COL", "filelist": [], "linkType": "collection"},
        "jobs": {"ext": "JOB", "filelist": []},
        "tasks": {"ext": "TSK", "filelist": [], "linkType": "task"},
        "sparkJobs": {
            "ext": "SPRK",
            "filelist": [],
            "api": "spark/configurations",
            "linkType": "spark",
        },
        "blobs": {"ext": "BLOB", "filelist": [], "linkType": "blob"},
        "features": {"ext": "CF", "filelist": []},
    }

    var_replacements = None
    replace_pattern = r"^\$\{(.*)\}$"
    # array of ext =  [ OBJ_TYPES[k]['ext'] for k in OBJ_TYPES.keys() ] or [v['ext'] for v in OBJ_TYPES.values() if 'ext' in v]
    # rework above to be keyed by extension v[1]['ext'] contining value of {type, filelist}
    EXT_FILES_MAP = dict(
        (v[1]["ext"], {"type": v[0], "filelist": v[1]["filelist"]}) for v in [v for v in OBJ_TYPES.items()]
    )
    TAG_SUFFIX: str = "_mergeForm"

    def get_suffix(type):
        if type in OBJ_TYPES:
            return "_" + OBJ_TYPES[type]["ext"] + ".json"
        else:
            eprint("ERROR: No object Suffix of '" + type + "' is registered in OBJ_TYPES. ")

    def is_substitution_type(type):
        if type in OBJ_TYPES and "substitute" in OBJ_TYPES[type] and OBJ_TYPES[type]:
            return True
        return False

    def get_file_list_for_type(type):
        if type in OBJ_TYPES:
            return OBJ_TYPES[type]["filelist"]
        else:
            eprint("ERROR: No object Suffix of '" + type + "' is registered in OBJ_TYPES. ")

    def get_api_for_type(type):
        api = None
        if type in OBJ_TYPES:
            type_obj: dict = OBJ_TYPES[type]
            api = type  # default api the same name as type
            if "api" in type_obj:
                api = type_obj["api"]
        else:
            eprint("ERROR: No api path registered for OBJ_TYPE[ '" + type + "' ]. ")
        return api

    # lookup the type of object based on the _SUFFIX.json SUFFIX from a file
    def infer_type_from_file(filename):
        obj_type = None
        path = filename.split("/")
        extension = os.path.splitext(path[-1])
        file = extension[0]
        ext = extension[-1]

        under_loc = file.rfind("_")
        if under_loc > 0 and under_loc < len(file):
            t_ext = file[under_loc + 1 :]
            if t_ext in EXT_FILES_MAP:
                obj_type = EXT_FILES_MAP[t_ext]["type"]
        return obj_type

    def get_versioned_api(types: dict, default: str):
        """
        Called when the OBJ_TYPES element for a given type uses a version specific api.
         - i.e. when needed api is different for 4.0.1 vs 4.0.2

        typeObj
         - will contain the OBJ_TYPES element for the given type.
         - must contain a versionedApi element.

        Depending on the version of the target Fusion machine, an api will be selected.

        If the target version is not listed the default will be returned.

        """
        api = default
        api_versions = types["versionedApi"]
        if fusion_version in api_versions:
            api = api_versions[fusion_version]
        elif "default" in api_versions:
            api = api_versions["default"]
        return api

    def init_args():
        global var_replacements
        debug("init_args top")
        env = {}  # some day we may get better environment passing

        # setting come from command line but if not set then pull from environment
        if args.server == None:
            args.server = init_args_from_maps("lw_OUT_SERVER", "localhost", os.environ, env)

        if args.user == None:
            args.user = init_args_from_maps("lw_USERNAME", "admin", os.environ, env)

        if args.password == None:
            args.password = init_args_from_maps("lw_PASSWORD", "password123", os.environ, env)

        if not os.path.isdir(args.dir):
            sys.exit("Can not find or access the " + args.dir + " directory. Process aborted. ")

        if args.varFile != None:
            if os.path.isfile(args.varFile):
                with open(args.varFile, "r") as jfile:
                    try:
                        var_replacements = json.load(jfile)
                    except Exception as e:
                        sprint(f"Problem parsing json from {args.varFile}, {str(e)}")
            else:
                sys.exit("Cannot find or access the " + args.varFile + " file.  Process aborted.")

    def init_args_from_maps(key, default, penv, env):
        if key in penv:
            debug("penv has_key" + key + " : " + penv[key])
            return penv[key]
        else:
            if key in env:
                debug("env has_key" + key + " : " + env[key])
                return env[key]
            else:
                debug("default setting for " + key + " : " + default)
                return default

    # if we are exporting an app then use the /apps/appname bas uri so that exported elements will be linked to the app
    def make_base_uri(force_legacy=False):
        base = args.server + "/api"

        if not app_name or force_legacy:
            uri = base
        else:
            uri = base + "/apps/" + app_name
        return uri

    def get_def_or_val(val, default):
        if val is None:
            return default
        return val

    def substitute_variable(obj, obj_name, var_map):
        if var_map and isinstance(obj, str) and re.search(replace_pattern, obj):
            match = re.search(replace_pattern, obj)
            group = match.group(1)
            if group in var_map:
                var = var_map[group]
                if var:
                    if args.verbose:
                        sprint("Substituting value in object " + obj_name + " for key: " + group)
                    obj = var
            else:
                eprint(f"Var replacement for file {obj_name} failed. Could not extract {group} from object element ")
        return obj

    def traverse_and_replace(obj, obj_name, var_map=None, path=None):
        # short circuit if we have no mappings
        if isinstance(var_map, dict):
            if path is None:
                path = []

            if isinstance(obj, dict):
                value = {k: traverse_and_replace(v, obj_name, var_map, path + [k]) for k, v in obj.items()}
            elif isinstance(obj, list):
                value = [traverse_and_replace(elem, obj_name, var_map, path + [[]]) for elem in obj]
            else:
                # search and see if our path is a ${var} match and if so replace with value from varFile
                value = substitute_variable(obj, obj_name, var_map)
        else:
            value = obj

        return value

    def do_http_post_put(url, data_file, is_put, headers=None, usr=None, pswd=None):
        usr = get_def_or_val(usr, args.user)
        pswd = get_def_or_val(pswd, args.password)
        extension = os.path.splitext(data_file)[1]
        auth = None
        if headers is None:
            headers = {}
            if extension == ".xml" or data_file.endswith("managed-schema"):
                headers["Content-Type"] = "application/xml"
            elif extension == ".json":
                headers["Content-Type"] = "application/json"
            else:
                headers["Content-Type"] = "text/plain"
        if args.jwt is not None:
            headers["Authorization"] = f"Bearer {args.jwt}"
        else:
            auth = requests.auth.HTTPBasicAuth(usr, pswd)

        files = None
        try:
            if os.path.isfile(data_file):
                with open(data_file, "rb") as payload:
                    if is_put:
                        response = requests.put(
                            url,
                            auth=auth,
                            headers=headers,
                            data=payload,
                            verify=is_verify(),
                        )
                    else:
                        response = requests.post(
                            url,
                            auth=auth,
                            headers=headers,
                            data=payload,
                            verify=is_verify(),
                        )

                    return response
            else:
                eprint("File '" + data_file + "' does not exist.  PUT/POST not performed.")
        except requests.ConnectionError as e:
            eprint(e)

    #  POST the given payload to apiUrl.  If it already exists then tack on the id to the URL and try a PUT
    def do_post_by_id_then_put(
        api_url,
        payload,
        obj_type,
        put_params="?_cookie=false",
        post_params="?_cookie=false",
        id_field="id",
        usr=None,
        pswd=None,
        exists_checker=None,
    ):
        if exists_checker == None:
            exists_checker = lambda response, payload: response.status_code == 409

        id = payload[id_field]

        if args.verbose:
            sprint("\nAttempting POST of " + obj_type + " definition for '" + id + "' to Fusion.")
        usr = get_def_or_val(usr, args.user)
        pswd = get_def_or_val(pswd, args.password)

        auth = None
        headers = {"Content-Type": "application/json"}
        if args.jwt is not None:
            headers["Authorization"] = f"Bearer {args.jwt}"
        else:
            auth = requests.auth.HTTPBasicAuth(usr, pswd)

        url = api_url
        if post_params is not None:
            url += "?" + post_params
        response = requests.post(url, auth=auth, headers=headers, data=json.dumps(payload), verify=is_verify())
        url = api_url
        if exists_checker(response, payload):
            if args.verbose:
                sprint("The " + obj_type + " definition for '" + id + "' exists.  Attempting PUT.")

            url = api_url + "/" + id
            if put_params is not None:
                url += "?" + put_params

            # if we got here then we tried posting but that didn't work so now we will try a PUT
            response = requests.put(
                url,
                auth=auth,
                headers=headers,
                data=json.dumps(payload),
                verify=is_verify(),
            )
            # If the PUT says the object exists,
            # then the likely problem is that  the object isn't linked to the current app.
            # Check and see if the response complains of the "id not in app" and add a link if needed.
            if f"{id} not in app" in response.text or re.search(
                f"The (Task|Collection|Data Model) with id '{id}' does not exist",
                response.text,
            ):
                lresponse = make_link(obj_type, id)
                if lresponse.status_code >= 200 and lresponse.status_code <= 250:
                    # try update again after link is in place
                    response = requests.put(
                        url,
                        auth=auth,
                        headers=headers,
                        data=json.dumps(payload),
                        verify=is_verify(),
                    )

        if response.status_code >= 200 and response.status_code <= 250:
            sprint("Element " + obj_type + " id: " + id + " PUT/POSTed successfully")
        elif response.status_code:
            eprint(
                "Non OK response of "
                + str(response.status_code)
                + " when doing PUT/POST to: "
                + api_url
                + "/"
                + id
                + " response.text: "
                + response.text
            )

        return response

    def put_blobs():

        blobdir = os.path.join(args.dir, "blobs")
        for f in get_file_list_for_type("blobs"):
            resource_type = None
            path = None
            content_type = None
            blob_id = None

            # read in json and figure out path and resourceType
            with open(os.path.join(args.dir, f), "r") as jfile:
                blobj = json.load(jfile)
                resource_type = None
                blob_id = blobj["id"]
                meta = blobj["metadata"]
                if meta:
                    if "resourceType" in meta:
                        resource_type = meta["resourceType"]
                    elif "type" in meta:
                        resource_type = meta["type"]

                content_type = blobj["contentType"]
                path = blobj["path"]

            url = make_base_uri(True) + "/blobs" + path
            if resource_type:
                url += "?resourceType=" + resource_type

            headers = {}
            if content_type:
                headers["Content-Type"] = content_type

            # convert unix path from json to os path which may be windows
            fullpath = blobdir + path.replace("/", os.sep)
            # now PUT blob
            if args.verbose:
                sprint("Uploading blob " + f)

            response = do_http_post_put(url, fullpath, True, headers)
            if response is not None and response.status_code >= 200 and response.status_code <= 250:
                if args.verbose:
                    sprint("Uploaded " + path + " payload successfully")
                # make_base_uri(True) is used for Fusion 4.0.1 compatibility but this requires us to make the link
                make_link("blobs", blob_id)

            elif response is not None and response.status_code:
                eprint("Non OK response: " + str(response.status_code) + " when processing " + f)

    def make_link(resourcetype, id):
        obj_type = None
        auth = None
        headers = {"Content-Type": "application/json"}
        if args.jwt is not None:
            headers["Authorization"] = f"Bearer {args.jwt}"
        else:
            auth = requests.auth.HTTPBasicAuth(args.user, args.password)

        if resourcetype in OBJ_TYPES and "linkType" in OBJ_TYPES[resourcetype]:
            obj_type = OBJ_TYPES[resourcetype]["linkType"]
        else:
            obj_type = resourcetype

        lurl = make_base_uri(True) + "/links"
        # {"subject":"blob:lucid.googledrive-4.0.1.zip","object":"app:EnterpriseSearch","linkType":"inContextOf"}
        payload = {"subject": "", "object": "", "linkType": "inContextOf"}
        payload["subject"] = f"{obj_type}:{id}"
        payload["object"] = f"app:{app_name}"
        lresponse = requests.put(
            lurl,
            auth=auth,
            headers=headers,
            data=json.dumps(payload),
            verify=is_verify(),
        )
        if lresponse and lresponse.status_code < 200 or lresponse.status_code > 250:
            eprint(
                "Non OK response: {}   when linking object {} to App {}".format(
                    str(lresponse.status_code), payload["subject"], app_name
                )
            )

        return lresponse

    def put_apps():
        global app_name
        app_files = get_file_list_for_type("fusionApps")
        # check and make sure we have no more than one app
        if len(app_files) == 1:
            # put the the name of the app in a global
            f = app_files[0]
            with open(os.path.join(args.dir, f), "r") as jfile:
                # Capture the id in the global so that /apps/APP_ID type urls can be used to maintain APP links
                payload = json.load(jfile)
                app_name = payload["id"]

        else:
            sys.exit(
                "Exactly one file with name ending in "
                + get_suffix("fusionApps")
                + " in directory "
                + args.dir
                + " is required! Exiting."
            )

        # GET from /api/apollo/apps/NAME to check for existence
        # POST to /api/apollo/apps?relatedObjects=false to write

        for f in app_files:
            apps_url = args.server + "/api"
            apps_url += "/apps"
            post_url = apps_url + "?relatedObjects=false"
            put_url = apps_url + "/" + app_name + "?relatedObjects=false"

            response = do_http(put_url)
            is_put = response and response.status_code == 200
            url = put_url if is_put else post_url

            response = do_http_post_put(url, os.path.join(args.dir, f), is_put)

            if response.status_code == 200:
                if args.verbose:
                    sprint("Created App " + app_name)
            elif response.status_code != 200:
                if hasattr(response, "reason"):
                    reason = response.reason
                else:
                    reason = "Unknown"
                if args.verbose:
                    print("...Failure.")
                    sys.exit(
                        f'"Non OK response of "{response.status_code}" when POSTing app: "{f}" \nReason: "{reason}"\nAborting...'
                    )

    def sort_schema_files(e):
        """
        Telling Solr to reload config with every schema file load is problematic.
          - slow
          - we don't know dependencies and reloading requires order

        However we have seen times when a synonyms or stopword file failed to load because a schema change
        had not been reloaded.  The 500 error complained about a missing znode called schema.xml

        While not perfect, ordering the files so that the end of the list has schema followed by solrconfig.xml
        seems to get around the ordering problem.  THis doesn't mean that a dependency going the other direction
        could not cause a problem though.  If that happens, a manual reload of individual files may be needed.

        :param e:
        :return:
        """
        # put signals and signals aggr collections first
        # Perhaps all auto-created collections should be listed here
        if not re.search("solrconfig.xml|schema", e):
            e = "1_" + e
        elif re.search("schema", e):
            e = "2_" + e
        elif re.search("schema", e):
            e = "3_" + e
        return e

    def sort_collection(e):
        # put signals and signals aggr collections first
        # Perhaps all auto-created collections should be listed here
        expr = "_signals_|_rewrite_"
        if app_name is not None:
            expr = expr + f"|^{app_name}_COL.json$|collections/{app_name}_COL.json$"
        if re.search(expr, e):
            e = "1_" + e
        return e

    # GET the feature from the target and see if it's identical to what's queued for upload
    def is_duplicate_feature(url, feature, usr=None, pswd=None):
        usr = get_def_or_val(usr, args.user)
        pswd = get_def_or_val(pswd, args.password)
        auth = None
        headers = {"Content-Type": "application/json"}
        if args.jwt is not None:
            headers["Authorization"] = f"Bearer {args.jwt}"
        else:
            auth = requests.auth.HTTPBasicAuth(usr, pswd)

        try:
            response = requests.get(url, auth=auth, headers=headers, verify=is_verify())
            response.raise_for_status()
            current_feature = json.loads(response.content)
            if args.debug:
                sprint(f"feature duplicate check = {current_feature == feature}")
            return current_feature == feature

        except Exception as ex:
            pass

        return False

    def put_features():
        api_url = make_base_uri() + "/collections"
        files = get_file_listing(os.path.join(args.dir, "collectionFeatures"), [])
        colfiles_o = get_file_list_for_type("collections")
        colfiles = []
        for c in colfiles_o:
            if c.startswith("collections"):
                # remove an extra char for the path delim
                c = c.split("collections")[1][1:]
            colfiles.append(os.path.join(args.dir, "collectionFeatures", c.split("_COL.json")[0]))

        params = "_cookie=false"
        for f in files:
            # only process features that have a matching collection upload in the file list
            if f.endswith(f'{get_suffix("features")}') and f.split(get_suffix("features"))[0] in colfiles:

                with open(f, "r") as jfile:
                    payload = json.load(jfile)
                    for feature in payload:
                        name = feature["name"]
                        col = feature["collectionId"]
                        url = f"{api_url}/{col}/features/{name}"
                        if is_duplicate_feature(url, feature):
                            if args.verbose:
                                sprint(f'Skipping "{name}" feature for collection "{col}" because it is unchanged.')
                            continue
                        try:
                            response = do_http_json_put(url, feature)
                            response.raise_for_status()
                            sprint(f'Successfully uploaded "{name}" feature for collection "{col}"')

                        except Exception as ex:
                            # some exceptions are ok because Fusion sends a 500 error if it can't delete non-existing collections
                            ex_text = ""
                            if hasattr(ex, "text"):
                                ex_text = ex["text"]
                            elif hasattr(ex, "response") and hasattr(ex.response, "text"):
                                ex_text = ex.response.text

                            search = re.search("Unable to (create|delete) (.*) collection", ex_text)

                            if search:
                                sprint(
                                    f'WARNING: dependent collection not deleted/created when feature "{name}" uploaded for collection "{col}"'
                                )
                            else:
                                eprint(f'Error putting "{name}" feature for collection "{col}". msg:\n\t{ex_text}')

    def put_collections():
        api_url = make_base_uri() + "/collections"
        sorted_files = sorted(get_file_list_for_type("collections"), key=sort_collection)
        params = "_cookie=false"
        for f in sorted_files:
            with open(os.path.join(args.dir, f), "r") as jfile:
                payload = json.load(jfile)
                # pop off name for collections pointing at "default".  That way the local collections get created in Solr.
                # keep the name for external (non-default) collections since those only need the Fusion reference created.

                do_pop = payload["solrParams"] and payload["searchClusterId"] == "default"

                # also keep if solrParams.name != the fusion name "id" and args.
                if payload["solrParams"] and payload["id"] != payload["solrParams"]["name"]:
                    do_pop = False
                    debug("Not creating Solr collection named " + payload["solrParams"]["name"])
                if payload["type"] is not None and payload["type"] == "DATA":
                    params += "&defaultFeatures=false"

                if do_pop:
                    payload["solrParams"].pop("name", None)
                if payload["searchClusterId"] == "default":
                    # to skip sub collections add defaultFeatures=false
                    response = do_post_by_id_then_put(
                        api_url,
                        payload,
                        "Collection",
                        put_params=params,
                        post_params=params,
                    )
                    if response.status_code == 200:
                        if args.verbose:
                            sprint(f'Successfully uploaded collection definition for {payload["id"]}')

                        put_schema(payload["id"])

    #
    # invert the args.noVerify for readability
    #
    def is_verify():
        return not args.noVerify

    def do_http(url, usr=None, pswd=None):
        usr = get_def_or_val(usr, args.user)
        pswd = get_def_or_val(pswd, args.password)
        auth = None
        headers = {}
        if args.jwt is not None:
            headers["Authorization"] = f"Bearer {args.jwt}"
        else:
            auth = requests.auth.HTTPBasicAuth(usr, pswd)

        response = None
        try:
            response = requests.get(url, auth=auth, headers=headers, verify=is_verify())
            return response
        except requests.ConnectionError as e:
            eprint(e)

    def do_http_json_get(url):
        response = do_http(url)
        if response.status_code == 200:
            j = json.loads(response.content)
            return j
        else:
            if response.status_code == 401 and "unauthorized" in response.text:
                eprint(
                    "Non OK response of " + str(response.status_code) + " for URL: " + url + "\nCheck your password\n"
                )
            else:
                eprint("Non OK response of " + str(response.status_code) + " for URL: " + url)

    def get_file_listing(path, remove_path_prefix=True):
        """
        walk the file system and grab files

        :param path:
        :param file_list:
        :return: file_list relative to the working directory
        """
        # for root, dirs, files in os.walk(path,topdown=True):
        # for directory in dirs:
        # get_file_listing(os.path.join(path,directory),file_list,directory)
        #    pass
        #    for file in files:
        #        addFile = os.path.join(dirs,file)
        #        if addFile not in file_list:
        #          file_list.append(addFile)
        path_list = list(Path(path).rglob("*"))
        # now make it a list of strings instead of posixPath objects
        file_list = []
        for p in path_list:
            if p.is_file():
                fstr = str(p)
                if remove_path_prefix and len(path) > 0 and fstr.startswith(path):
                    # add 1 to the len of the path to remove the path separator
                    fstr = fstr[len(path) + 1 :]
                file_list.append(fstr)
        return file_list

    def put_schema(col_name):
        schema_url = args.server + "/api"
        schema_url += "/collections/" + col_name + "/solr-config"
        current_zk_files = []
        # Get a listing of current files via Fusion's solr-config api.
        # This prevents uploading directories which don't exist (unsupported).
        # This listing needs to look like the relative path of the files themselves,
        # so that an existence check can be done for Put vs Post.
        zk_files_json = do_http_json_get(schema_url + "?recursive=true")
        if zk_files_json and len(zk_files_json) > 0:
            for obj in zk_files_json:
                if not obj["isDir"]:
                    current_zk_files.append(obj["name"])
                else:
                    for child in obj["children"]:
                        if not child["isDir"]:
                            current_zk_files.append(os.path.join(obj["name"], child["name"]))

        path_dir = os.path.join(args.dir, "configsets", col_name)
        files = sorted(get_file_listing(path_dir), key=sort_schema_files)

        counter = 0

        if len(files) > 0:
            sprint("\nUploading Solr config for collection: " + col_name)
        for file in files:
            counter += 1
            # if the file is part of the current configset and is available for upload, upload it.
            path_file = os.path.join(args.dir, "configsets", col_name, file)
            if os.path.isfile(path_file):
                is_last = len(files) == counter
                # see if the file exists and PUT or POST accordingly
                url = schema_url + "/" + file.replace(os.sep, "/")
                if is_last:
                    url += "?reload=true"
                # PUT to update, POST to add
                try:
                    response = do_http_post_put(url, path_file, (file in current_zk_files))
                    response.raise_for_status()
                    if args.verbose:
                        sprint("\tUploaded " + file + " successfully")
                        if is_last:
                            sprint("\tSent reload=true to collection " + col_name)
                except Exception as e:

                    if hasattr(e, "response") and e.response.status_code:
                        eprint("Non OK response: " + str(e.response.status_code) + " when uploading " + file)
                    elif hasattr(e, "response"):
                        r = e.response
                        msg = None
                        if hasattr(r, "msg"):
                            msg = r.msg
                        elif hasattr(e, "text"):
                            msg = r["text"]
                        else:
                            msg = str(e)
                            eprint(f"Error uploading {col_name} configset file {file}. Msg: {msg}")
                    else:
                        msg = str(e)
                        eprint(f"Error uploading {col_name} configset file {file}. Msg: {msg}")
            else:
                sprint(f"WARN: scan of {path_dir} for files found non-file {file}")

    def eprint(*params, **kwargs):
        print(*params, file=sys.stderr, **kwargs)
        if args.failOnStdError:
            sys.exit("Startup argument --failOnStdErr set, exiting putApp")

    def sprint(msg):
        # change inputs to *args, **kwargs in python 3
        # print(*args, file=sys.stderr, **kwargs)
        print(msg)
        sys.stdout.flush()

    def debug(msg):
        if args.debug:
            sprint(msg)

    # populate the fileList global with arrays of files to put
    def find_files():
        files = os.listdir(args.dir)
        for f in files:
            infer_type = infer_type_from_file(f)
            if infer_type:
                # grab the global filelist array for this type and stuff in the filename
                flist = get_file_list_for_type(infer_type)
                if isinstance(flist, list):
                    flist.append(f)
            elif f in OBJ_TYPES:
                dfiles = os.listdir(os.path.join(args.dir, f))
                for df in dfiles:
                    infer_type = infer_type_from_file(df)
                    if infer_type:
                        # grab the global filelist array for this type and stuff in the filename
                        flist = get_file_list_for_type(infer_type)
                        if isinstance(flist, list):
                            flist.append(os.path.join(f, df))

    def put_job_schedules():
        obj_type = "jobs"
        api_url = make_base_uri() + "/" + get_api_for_type(obj_type) + "/"
        for f in get_file_list_for_type(obj_type):
            with open(os.path.join(args.dir, f), "r") as jfile:
                payload = json.load(jfile)
                url = api_url + payload["resource"] + "/schedule"
                response = do_http_post_put(url, os.path.join(args.dir, f), True)
                if response.status_code == 200:
                    if args.verbose:
                        sprint("Created/updated Job schedule from " + f)
                # allow a 404 since we are using the /apollo/apps/{collection} endpoint,
                # but the export gives us global jobs as well
                elif response.status_code != 200 and response.status_code != 404:
                    eprint("Non OK response of " + str(response.status_code) + " when PUTing: " + url)

    def merge_readable_script(element, raw_tag):
        merge_tag = raw_tag + TAG_SUFFIX
        if merge_tag in element:
            script = "\n".join(element[merge_tag])
            element[raw_tag] = script  # overwrite script with the version used in diff/merge
            element.pop(merge_tag, None)

    def migrate_readable_script(data, obj_type):
        xform_tags = [
            "script" + TAG_SUFFIX,  # scala script, python
            "transformScala" + TAG_SUFFIX,  # PBL
            "transformSQL" + TAG_SUFFIX,  # PBL
            "sql" + TAG_SUFFIX,  # sqlTemplate
            "sparkSQL" + TAG_SUFFIX,  # sqlTemplate, headTail, tokenPhraseSpellCorrection
            "misspellingSQL" + TAG_SUFFIX,  # synonym detection
            "phraseSQL" + TAG_SUFFIX,  # synonym detection
            "rollupSql" + TAG_SUFFIX  # sql_template
            # ,"analyzerConfigQuery" + TAG_SUFFIX # candidate json from some 4 sisters
            # ,"notes" + TAG_SUFFIX # candidate for multi-line descript/notes field
        ]

        if isinstance(data, dict) and obj_type.endswith("Pipelines") and ("stages" in data):
            for stage in data["stages"]:
                if isinstance(stage, dict) and ("script" in stage):
                    if ("script" + TAG_SUFFIX) in stage:
                        merge_readable_script(stage, "script")
                    if ("condition" + TAG_SUFFIX) in stage:
                        merge_readable_script(stage, "condition")
        elif isinstance(data, dict) and obj_type == "sparkJobs":
            for tag in xform_tags:
                if tag in data:
                    merge_readable_script(data, tag.split("_")[0])

    def put_file_for_type(type, force_legacy=False, id_field=None, exists_checker=None):
        if not id_field:
            id_field = "id"
        api_url = make_base_uri(force_legacy) + "/" + get_api_for_type(type)
        for f in get_file_list_for_type(type):
            with open(os.path.join(args.dir, f), "r") as jfile:
                payload = json.load(jfile)
                if is_substitution_type(type):
                    if args.verbose and isinstance(var_replacements, dict):
                        sprint("Doing substitution for file " + f)
                    payload = traverse_and_replace(payload, f, var_replacements)

                migrate_readable_script(payload, type)
                # do_post_by_id_then_put(api_url, payload, type,None, id_field)
                do_post_by_id_then_put(
                    api_url,
                    payload,
                    type,
                    None,
                    None,
                    id_field,
                    None,
                    None,
                    exists_checker,
                )

    def put_template_file_for_type(obj_type, id_field=None, exists_checker=None):
        if not id_field:
            id_field = "id"
        base = args.server + "/"
        api_url = base + get_api_for_type(obj_type)
        for f in get_file_list_for_type(obj_type):
            with open(os.path.join(args.dir, f), "r") as jfile:
                payload = json.load(jfile)
                if is_substitution_type(obj_type):
                    if args.verbose is not None and isinstance(var_replacements, dict):
                        sprint("Doing substitution for file " + f)
                    payload = traverse_and_replace(payload, f, var_replacements)

                migrate_readable_script(payload, obj_type)
                # do_post_by_id_then_put(api_url, payload, type,None, id_field)
                do_post_by_id_then_put(
                    api_url,
                    payload,
                    obj_type,
                    None,
                    None,
                    id_field,
                    None,
                    None,
                    exists_checker,
                )

    def do_http_json_put(url, payload, usr=None, pswd=None):
        usr = get_def_or_val(usr, args.user)
        pswd = get_def_or_val(pswd, args.password)
        headers = {"Content-Type": "application/json"}
        auth = None
        if args.jwt is not None:
            headers["Authorization"] = f"Bearer {args.jwt}"
        else:
            auth = requests.auth.HTTPBasicAuth(usr, pswd)
        try:
            response = requests.put(url, auth=auth, headers=headers, data=json.dumps(payload))
            return response
        except requests.ConnectionError as e:
            eprint(e)

    def fetch_fusion_version():
        global fusion_version
        url = make_base_uri() + "/configurations"
        configurations = do_http_json_get(url)
        if configurations is not None and configurations["app.version"]:
            fusion_version = configurations["app.version"]

    def main():
        init_args()
        fetch_fusion_version()

        # fetch collections first
        sprint("Uploading objects found under '" + args.dir + "' to Fusion version " + fusion_version)
        sprint(f"Upload target API: {make_base_uri()}")

        find_files()
        # put_apps must be the first export, clusters next.  blobs and collections in either order then pipelines
        put_apps()

        put_collections()
        put_features()
        put_blobs()

        put_file_for_type("parsers")
        put_file_for_type("indexPipelines")
        put_file_for_type("queryPipelines")
        put_file_for_type("indexProfiles")
        put_file_for_type("queryProfiles")
        put_file_for_type("tasks")
        put_file_for_type("sparkJobs", None, None, lambda r, p: spark_checker(r, p))

        put_file_for_type("dataSources", None, None, lambda r, p: datasource_checker(r, p))
        put_job_schedules()

        put_template_file_for_type("zones")
        put_template_file_for_type("templates")
        put_file_for_type("dataModels")

    def spark_checker(response, payload):
        exists = False
        status = response.status_code
        text = response.text
        exists = status == 409
        if not exists:
            exists = (status == 500 or status == 400) and text.find(payload["id"] + " already exists") > 0
        return exists

    def datasource_checker(response, payload):
        # old fusion sends 409, 4.1 500, 4.2 400
        exists = False
        status = response.status_code
        text = response.text
        exists = status == 409
        if not exists:
            exists = (status == 500 or status == 400) and text.find(
                "Data source id '" + payload["id"] + "' already exists"
            ) > 0
        return exists

    if __name__ == "__main__":
        scriptName = os.path.basename(__file__)
        # sample line: 'usage: putProject.py [-h] [-d DIR] [-s SERVER]'
        description = (
            "______________________________________________________________________________\n"
            "Take a folder containing .json files or directories of .json files such as that  \n"
            " (produced by get_fusion_app.py) and POST the contents to a running Fusion instance.  \n"
            "Fusion may create App and OOTB Collection definitions if needed but files under \n"
            "the --dir folder may overwrite portions of these auto-create objects. \n"
            "______________________________________________________________________________"
        )

        parser = argparse.ArgumentParser(description=description, formatter_class=RawTextHelpFormatter)

        parser.add_argument(
            "-d",
            "--dir",
            help="Input directory (with a *_APP.json file), required.",
            required=True,
        )  # ,default="default"
        parser.add_argument(
            "--failOnStdError",
            help="Exit the program if StdErr is written to i.e. fail when any call fails.",
            default=False,
            action="store_true",
        )
        parser.add_argument(
            "-s",
            "--server",
            metavar="SVR",
            help="Fusion server to send data to. Default: ${lw_OUT_SERVER} or 'localhost'.",
        )  # default="localhost"
        parser.add_argument("-u", "--user", help="Fusion user, default: ${lw_USER} or 'admin'.")  # ,default="admin"
        parser.add_argument(
            "--password",
            help="Fusion password,  default: ${lw_PASSWORD} or 'password123'.",
        )  # ,default="password123"
        parser.add_argument(
            "--jwt",
            help="JWT token for authentication.  If set, --password is ignored",
            default=None,
        )

        parser.add_argument(
            "--debug",
            help="Print debug messages while running, default: False.",
            default=False,
            action="store_true",
        )  # default=False
        parser.add_argument(
            "--noVerify",
            help="Do not verify SSL certificates if using https, default: False.",
            default=False,
            action="store_true",
        )  # default=False
        parser.add_argument(
            "-v",
            "--verbose",
            help="Print details, default: False.",
            default=False,
            action="store_true",
        )  # default=False
        parser.add_argument(
            "--varFile",
            help="Protected variables file used for password replacement (if needed) default: None.",
            default=None,
        )

        args = parser.parse_args()
        main()

except ImportError as ie:
    print(
        "Failed to Import from module: ",
        ie.name,
        "\ninstall the module via the pip installer\n\nExample:\n\t$ pip3 install ",
        ie.name,
        file=sys.stderr,
    )
    sys.exit(1)
except Exception as e:
    msg = None
    if hasattr(e, "msg"):
        msg = e.msg
    elif hasattr(e, "text"):
        msg = e["text"]
    elif hasattr(e, "txt"):
        msg = e["txt"]
    else:
        msg = str(e)

    print("Exception: " + msg, file=sys.stderr)
    sys.exit("1")
