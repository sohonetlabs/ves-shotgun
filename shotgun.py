# ---------------------------------------------------------------------
# Shotgunves.py written by Mike Romey, Martin Rushworth and Ben Roeder.
#
# Requires shotgun API: https://github.com/shotgunsoftware/python-api
# Reference methods:
# https://github.com/shotgunsoftware/python-api/wiki/Reference%3A-Methods
#
# Class ShotgunVES wraps the shotgun API with helping methods specifically
# for the VES Shotgun and Sohonet databases.
# The __main__ method should you run this as a script looks at django model
# ShotgunJob's to update shotgun.
#
# You can access the shotgun API directly through the ShotgunVES class with
# the correct settings to connect to the VES Shotgun database, example:
#
# from shotgunves import ShotgunVES
# projectInfo  = { 'type': 'Project', 'id': 68 } # 68 is the 2012 VES project
# shotgun = ShotgunVES(projectInfo)
# connected = shotgun.connect()
# shotgun.sg.<any shotgun API call>
#
# Edited by Mark McArdle 28-10-2014
#
# ------------------------------------------------------------------
# Imports
# ------------------------------------------------------------------

import os
import re
import traceback
import logging
import tempfile
from shotgun_api3 import Shotgun
from django.conf import settings
from swiftclient import ClientException
from ves.awards.models import Entry, EntryFiles
from ves.awards.views import genSlate
from applications import models as app_models
from sohonet_encode.movtool import MovFile

import operator
import time

SHOTGUN_PROJECT_ID = settings.SHOTGUN_PROJECT_ID
PROJECT_INFO = {'type': 'Project', 'id': SHOTGUN_PROJECT_ID}

MAIN_PROXY_NAME = settings.VES_MAIN_PROXY_NAME

# -----------------------------------------------------------------
# Globals
# -----------------------------------------------------------------

SERVER_PATH = 'https://ves.shotgunstudio.com'
SCRIPT_USER = 'createSubmission'
SCRIPT_KEY = ''

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def log_exception(message):
    trace = '%s\n%s' % (message, traceback.format_exc())
    _m = "VES Shotgun: %s" % trace
    logger.error(_m)


def log_error(message):
    _m = "VES Shotgun: %s" % message
    logger.error(_m)


def log_debug(message):
    _m = "VES Shotgun: %s" % message
    logger.debug(_m)


def log(message):
    _m = "VES Shotgun: %s" % message
    logger.info(_m)


def get_oldest_file(files, _invert=False):
    """ Find and return the oldest file of input file names.
    Only one wins tie. Values based on time distance from present.
    Use of `_invert` inverts logic to make this a youngest routine,
    to be used more clearly via `get_youngest_file`.
    """
    gt = operator.lt if _invert else operator.gt
    # Check for empty list.
    if not files:
        return None
    # Raw epoch distance.
    now = time.time()
    # Select first as arbitrary sentinel file, storing name and age.
    oldest = files[0], now - os.path.getctime(files[0])
    # Iterate over all remaining files.
    for f in files[1:]:
        age = now - os.path.getctime(f)
        if gt(age, oldest[1]):
            # Set new oldest.
            oldest = f, age
    # Return just the name of oldest file.
    return oldest[0]


def get_youngest_file(files):
    return get_oldest_file(files, _invert=True)


def download_from_swift(upload_object, path):
    user_package = upload_object.application_user_package
    package = user_package.package
    storage_location = package.storage_location
    connection = storage_location.get_connection()

    container = settings.VES_PROXY_CONTAINER
    obj_name = os.path.basename(path)
    full_path = u'%s/%s' % (container, obj_name)
    log('Downloading %s' % full_path)
    try:
        connection.head_object(container, obj_name)
        log(u"Swift Object Exists %s" % full_path)

        f = open(path, 'wb')
        _, ob_contents = connection.get_object(
            container=container,
            obj=obj_name,
            resp_chunk_size=(1024 * 1024 * 40))

        for chunk in ob_contents:
            f.write(chunk)

        f.close()

        log(u"Swift Download Complete %s" % full_path)

    except ClientException as ce:
        log_exception(ce)

# ShotgunVES wraps the shotgun API with helping methods specifically for the
# VES Shotgun and Sohonet databases.
class ShotgunVES:

    project_info = {}  # Tells shotgun what project we want to work on.
    sg = None  # Shotgun API object

    def __init__(self, project_info):
        self.project_info = project_info

    # Connect to the Shotgun Database
    def connect(self):
        try:
            log("Connecting to %s..." % (SERVER_PATH))
            self.sg = Shotgun(SERVER_PATH, SCRIPT_USER, SCRIPT_KEY)
            log("Connected")
            return True
        except Exception, e:
            log("Unable to connect to Shotgun server. %s" % e)
            return False

    # Get the correct category identifiers from a sohonet entry object
    def get_category(self, entry):
        category = self.sg.find_one(
            'Shot',
            [
                ['project', 'is',
                    {'type': 'Project', 'id': self.project_info['id']}],
                ['code', 'contains',
                 'Cat' + str(entry.entryNum.category.catNum).zfill(3)]],
            ['id', 'code', 'project']
        )
        return category

    # See if a company is already in the database,
    # if so returns the identifiers
    def get_company(self, company):
        data = self.sg.find_one(
            'CustomNonProjectEntity01', [['code', 'is', company]], ['id'])
        if data and company != '':
            data = self.sg.create(
                'CustomNonProjectEntity01', {'code': company})
        if data:
            return {'type': 'CustomNonProjectEntity01', 'id': data['id']}
        else:
            return None

    # Specific VES requirement
    def get_clean_state(self, state):
        if state == 'California':
            return 'CA'
        elif state == 'New York':
            return 'NY'
        else:
            return state

    # Retires an entry, can be reversed under most circumstances
    # see the API documentation
    def retire_entry(self, entryNum):
        entry = self.sg.find_one('Version', [['project', 'is',
                                              {'type': 'Project',
                                               'id': self.project_info['id']}],
                                             ['code', 'is', entryNum]],
                                 ['id', 'code'])
        if entry is None:
            return None
        else:
            self.sg.delete('Version', entry['id'])
            return entry['id']

    # Returns a user from the data, either an existing one or one that it
    # creates in this method
    def get_user(self, userData):
        if userData['firstname'] != '' and userData['lastname'] != '':
            userLogin = (re.sub(r'\s+', '', userData['firstname'].lower())
                         + '_' + re.sub(r'\s+', '',
                                        userData['lastname'].lower()))
            userLogin = userLogin.replace('.', '')
            userLogin = userLogin.replace('-', '')
            userData['login'] = userLogin
            userInfo = self.sg.find_one(
                'HumanUser',
                [['login', 'is', userLogin]], ['groups', 'projects'])
            if userInfo is None:
                userData['login'] = userLogin
                userData['projects'] = [self.project_info]
                userData['sg_status_list'] = 'dis'
                userData = self.sg.create('HumanUser', userData)
                return userData
            else:
                # Add New Projects
                projectList = [self.project_info]
                thisProjectInfo = (userInfo['projects'])
                for project in thisProjectInfo:
                    projectList.extend([{'type': 'Project', 'id': project['id']}])
                userData['projects'] = projectList

                # Update User
                userData = self.sg.update(
                    'HumanUser', userInfo['id'], userData)
                return userData

    # get some default vetting fields to attach for new entries.
    def get_vetting_check_list(self):
        taskTemplate = self.sg.find_one(
            "TaskTemplate",
            filters=[['code', 'is', 'vettingCheckList']], fields=['id'])
        return taskTemplate

    # Generate a VES shotgun specific 'contact' dictionary from a
    # sohonet entry object
    def generateContactData(self, entry):
        sg_job_title = ''
        country = unicode(entry.submissionContact.country).encode(
            "ISO-8859-1", 'ignore')
        contactData = {
            'sg_address': entry.submissionContact.streetAddress,
            'sg_apt': entry.submissionContact.suite,
            'sg_city': entry.submissionContact.city,
            'sg_country': country,
            'sg_zip': entry.submissionContact.zipOrPostCode,
            'email': entry.submissionContact.emailAddr,
            'firstname': entry.submissionContact.firstName,
            'lastname': entry.submissionContact.lastName,
            'sg_fax': entry.submissionContact.fax,
            'sg_memo_id': str(entry.submissionContact.vfxMemberNum),
            'sg_job_title': sg_job_title,
            'sg_state': self.get_clean_state(
                entry.submissionContact.stateProvince),
            'sg_phone': entry.submissionContact.primaryPhone,
        }
        cintactInfo = self.get_user(contactData)
        contactData = {'type': 'HumanUser', 'id': cintactInfo['id']}

        return contactData

    # Generate a VES shotgun specific list of entrant dictionaries from a
    # sohonet entry object
    def generateEntrantData(self, entry):
        # a list of dictionary's, containing entrant information
        entrantDetailsList = []
        entrants = []
        entrantDict = {
            'entrant_1': None,
            'entrant_1_job_title': None,
            'entrant_1_url': None,
            'entrant_2': None,
            'entrant_2_job_title': None,
            'entrant_2_url': None,
            'entrant_3': None,
            'entrant_3_job_title': None,
            'entrant_3_url': None,
            'entrant_4': None,
            'entrant_4_job_title': None,
            'entrant_4_url': None,
            'entrant_5': None,
            'entrant_5_job_title': None,
            'entrant_5_url': None,
        }
        model_identifier = 1
        if entry.entrant1:
            entrants.append((entry.entrant1, model_identifier))
        model_identifier += 1
        if entry.entrant2:
            entrants.append((entry.entrant2, model_identifier))
        model_identifier += 1
        if entry.entrant3:
            entrants.append((entry.entrant3, model_identifier))
        model_identifier += 1
        if entry.entrant4:
            entrants.append((entry.entrant4, model_identifier))
        model_identifier += 1
        if entry.entrant5:
            entrants.append((entry.entrant5, model_identifier))

        entrant_pos = 0
        for entrant, model_identifier in entrants:
            entrant_pos += 1
            jobTitleOrCredit = getattr(
                entry, 'e%sjobTitleOrCredit' % model_identifier, '')
            url = getattr(
                entry, 'e%sURL' % model_identifier, 'http://tempuri.com')

            if url:
                url_data = {
                    'content_type': "string",
                    'link_type': "url",
                    'name': "sg_credit_url",
                    'url': url
                }
            else:
                url_data = None
            country = unicode(entrant.country).encode("ISO-8859-1", 'ignore')
            entrantData = {
                'sg_address': entrant.streetAddress,
                'sg_apt': entrant.suite,
                'sg_city': entrant.city,
                'sg_country': country,
                'sg_zip': entrant.zipOrPostCode,
                'email': entrant.emailAddr,
                'firstname': entrant.firstName,
                'lastname': entrant.lastName,
                'sg_fax': entrant.fax,
                'sg_memo_id': str(entrant.vfxMemberNum),
                'sg_job_title': jobTitleOrCredit,
                'sg_state': self.get_clean_state(entrant.stateProvince),
                'sg_phone': entrant.primaryPhone,
                'sg_entrant_number': entrant_pos,
                'sg_credit_url': url_data,
            }
            entrantInfo = self.get_user(entrantData)

            if entrantInfo is not None:
                entrantDetailsList.extend(
                    [{'type': 'HumanUser', 'id': entrantInfo['id']}])
            entrantDict['entrant_'+str(entrant_pos)] = \
                {'type': 'HumanUser', 'id': entrantInfo['id']}
            _jt_index = 'entrant_%s_job_title' % entrant_pos
            _url_index = 'entrant_%s_url' % entrant_pos
            entrantDict[_jt_index] = jobTitleOrCredit
            entrantDict[_url_index] = url_data

        return entrantDetailsList, entrantDict

    # Generate a VES shotgun specific list of signature dictionaries from a
    # sohonet entry object
    def generateSignatureData(self, entry):
        signatureDetailsList = []
        signatures = []
        if entry.submittingEntrant:
            signatures.append(entry.submittingEntrant)
        if entry.entrantVFX:
            signatures.append(entry.entrantVFX)
        if entry.entrantFacilityMgr:
            signatures.append(entry.entrantFacilityMgr)
        sg_sig_number = 0
        for entrant in signatures:
            sg_sig_number += 1
            e_id = sg_sig_number + 1
            jobTitleOrCredit = getattr(entry, 'e%sjobTitleOrCredit' % e_id, '')
            signatureData = {
                'sg_country': unicode(entrant.country).encode("ISO-8859-1",
                                                              'ignore'),
                'email': entrant.emailAddr,
                'firstname': entrant.firstName,
                'lastname': entrant.lastName,
                'sg_fax': entrant.fax,
                'sg_phone': entrant.primaryPhone,
                'sg_job_title': jobTitleOrCredit,
                'sg_signature_number': sg_sig_number,
            }
            signatureInfo = self.get_user(signatureData)
            if signatureInfo is not None:
                signatureDetailsList.extend(
                    [{'type': 'HumanUser', 'id': signatureInfo['id']}])

        return signatureDetailsList

    # Generate a VES shotgun specific submit dictionary, this dictionary is
    # then the main object to create a submission in shotgun.
    def generateSubmissionData(self, entry, vettingList, entrantDetailsList,
                               entrantDict, signatureDetailsList,
                               contactData, category):

        audio = entry.no_audio
        ba_audio = entry.no_audio_ba

        submitData = {
            'code': unicode(entry.entryNum).encode("ISO-8859-1", 'ignore'),
            'sg_entry_title': entry.sequenceOrShotname,
            'sg_project_title': entry.projectName,
            'sg_premiere_date': entry.dateOfPremiere.strftime('%Y-%m-%d'),
            'project': self.project_info,
            'entity': category,
            'sg_distribution_company': self.get_company(
                entry.distributionCompany),
            'sg_production_company': self.get_company(entry.productionCompany),
            'sg_terms_aggred': True,
            'sg_facility_employed': entry.entryAtFacility,
            'sg_petition': False,  # don't think we record this...
            'sg_submitter_list': entrantDetailsList,
            'sg_signature_list': signatureDetailsList,
            'sg_contact': contactData,
            'sg_time_to_screen_submissions': 600000,
            'sg_time_to_vote': 300000,
            'sg_time_to_read_suppliments': 90000,
            'task_template': vettingList,
            'sg_soho_updated': entry.lastEdit,
            'sg_payment': entry.hasPaid,
            'sg_payment_amount': float(entry.getPrice()),
            'sg_entry_mos': audio,
            'sg_bna_mos': ba_audio
        }
        submitData['sg_submitter_1'] = entrantDict['entrant_1']
        submitData['sg_submitter_1_url'] = entrantDict['entrant_1_url']
        submitData['sg_submitter_1_job_title'] = entrantDict['entrant_1_job_title']
        submitData['sg_submitter_2'] = entrantDict['entrant_2']
        submitData['sg_submitter_2_url'] = entrantDict['entrant_2_url']
        submitData['sg_submitter_2_job_title'] = entrantDict['entrant_2_job_title']
        submitData['sg_submitter_3'] = entrantDict['entrant_3']
        submitData['sg_submitter_3_url'] = entrantDict['entrant_3_url']
        submitData['sg_submitter_3_job_title'] = entrantDict['entrant_3_job_title']
        submitData['sg_submitter_4'] = entrantDict['entrant_4']
        submitData['sg_submitter_4_url'] = entrantDict['entrant_4_url']
        submitData['sg_submitter_4_job_title'] = entrantDict['entrant_4_job_title']
        submitData['sg_submitter_5'] = entrantDict['entrant_5']
        submitData['sg_submitter_5_url'] = entrantDict['entrant_5_url']
        submitData['sg_submitter_5_job_title'] = entrantDict['entrant_5_job_title']

        return submitData

    # return a current submission if it exists, otherwise return None
    def getSubmitInfo(self, entry):
        return self.sg.find_one(
            'Version', [
                ['code', 'is',
                 unicode(entry.entryNum).encode("ISO-8859-1", 'ignore')],
                ['project', 'is',
                 {'type': 'Project', 'id': self.project_info['id']}]],
            ['id', 'image', 'sg_supplement_form', 'sg_entry_slate',
             'sg_bna_slate', 'sg_uploaded_movie', 'sg_uploaded_bna'])

    # This contains most of the syntax for taking an entry and updating shotgun
    def createUpdateShotgunEntry(self, entry, updateEntryDetails,
                                 updateEntryMedia, updateBAMedia,
                                 updateSuppMaterials):

        if entry.shotgunSync:
            log('Entry is marked to not update shotgun '
                'Entry.shotgunSync '+str(entry.entryNum))
            return 1

        if entry.hasBeenDeleted:
            log(
                'Entry has been deleted on Sohonet Site, '
                'retiring from shotgun ' + str(entry.entryNum))
            retire_result = self.retire_entry(str(entry.entryNum))
            if retire_result:
                log('Retired from shotgun ' + str(entry.entryNum))
            else:
                log('Could not retire from shotgun ' + str(entry.entryNum))
            return 1

        # ---------------------------------
        # if the entry details have been changed
        # ---------------------------------
        if updateEntryDetails:
            log('Updating entry details for '+str(entry.entryNum))

            category = self.get_category(entry)
            vettingList = self.get_vetting_check_list()

            if category is None:
                log('Could not find category for this entry, failed')
                return 1
            elif vettingList is None:
                log('Could not get a vetting list for the entry, failed')
                return 1
            else:
                entrantDetailsList, entrantDict = \
                    self.generateEntrantData(entry)
                signatureDetailsList = self.generateSignatureData(entry)
                contactData = self.generateContactData(entry)
                submitInfo = self.getSubmitInfo(entry)
                submitData = self.generateSubmissionData(
                    entry, vettingList, entrantDetailsList, entrantDict,
                    signatureDetailsList, contactData, category)
                if submitInfo is None:  # entry is not in shotgun yet
                    # need to create new
                    log('Creating new submission '+str(entry.entryNum))
                    submitData = self.sg.create('Version', submitData)
                else:
                    # entry is in shotgun,
                    # so need to update the existing entry
                    log('Updating submission '+str(entry.entryNum))
                    submitData = self.sg.update('Version', submitInfo['id'],
                                                submitData)
                # text on the slates may have changed so update these as well

                # genSlate is currently imported from awards/views.py,
                # thats where it was before, need to do this as the slate is
                # only saved in a directory when a user downloads it.

                aa_slate_contents = genSlate(entry.id, True)
                aa_tfile = tempfile.NamedTemporaryFile()
                aa_tfile_h = open(aa_tfile.name, 'wb')
                aa_tfile_h.write(aa_slate_contents)
                aa_tfile_h.close()
                entryShotgunName = str(entry.entryNum) + '.slateEntry.png'

                try:
                    self.sg.upload(
                        "Version", submitData['id'], aa_tfile.name,
                        "sg_entry_slate", entryShotgunName)
                except Exception as e:
                    log_exception(e)
                    log("Shotgun Upload Error on entry slate "
                        "for "+str(entry.entryNum)+' '+str(e))

                log("Uploading banda Slate")

                ba_slate_contents = genSlate(entry.id, False)
                ba_tfile = tempfile.NamedTemporaryFile()
                ba_tfile_h = open(ba_tfile.name, 'wb')
                ba_tfile_h.write(ba_slate_contents)
                ba_tfile_h.close()
                baShotgunName = str(entry.entryNum) + '.slateBNA.png'

                try:
                    self.sg.upload(
                        "Version", submitData['id'], ba_tfile_h.name,
                        "sg_bna_slate", baShotgunName)
                except Exception, e:
                    log_exception(e)
                    log("Shotgun Upload Error on banda slate "
                        "for "+str(entry.entryNum)+' '+str(e))

        # TODO - Should just be one storage location
        # TODO - find a better way of getting this.
        storage = app_models.ApplicationStorageLocation.objects.filter()[0]
        storage.ensure_container(settings.VES_PDF_CONTAINER)
        connection = storage.get_connection()

        # ---------------------------------
        # if the entry media has changed
        # ---------------------------------
        if updateEntryMedia:
            log('UPDATING ENTRY MEDIA FOR '+str(entry.entryNum))
            submitInfo = self.getSubmitInfo(entry)
            if submitInfo is None:  # no entry exists, fail
                log('Failed to find entry details in shotgun, '
                    'not uploading entry media for '+str(entry.entryNum))
                return 1
            else:
                # A lot of the following was taken from encode proxy.py
                entryFiles = EntryFiles(entry)
                entryFiles.findFiles()
                entryMd5 = entryFiles.getUserEntryMD5()

                if entryFiles.entry_name:
                    entryFilename = os.path.basename(entryFiles.entry_name)
                else:
                    entryFilename = None

                if entryMd5 is not None and entryFilename is not None:

                    entry_mp4_name = '%s.aa.%s.mov.%s.mp4' % (
                        entry.entryNum,
                        entryMd5,
                        MAIN_PROXY_NAME
                    )

                    if submitInfo['sg_uploaded_movie']:
                        nameInShotgun = submitInfo['sg_uploaded_movie']['name']
                    else:
                        nameInShotgun = None

                    if not nameInShotgun or not nameInShotgun.endswith(entry_mp4_name):

                        entry_temp_file = tempfile.NamedTemporaryFile(
                            suffix=entry_mp4_name
                        )
                        f = open(entry_temp_file.name, 'wb')
                        _, ob_contents = connection.get_object(
                            settings.VES_PROXY_CONTAINER,
                            entry_mp4_name,
                            resp_chunk_size=(1024 * 1024 * 40))

                        for chunk in ob_contents:
                            f.write(chunk)
                        f.close()

                        if os.path.exists(entry_temp_file.name):
                            log("Uploading %s " % entry_mp4_name)
                            try:
                                self.sg.upload("Version", submitInfo['id'],
                                               entry_temp_file.name,
                                               "sg_uploaded_movie",
                                               entry_mp4_name,
                                               entry_mp4_name
                                               )
                            except Exception, e:
                                log("Shotgun Upload Error on entry media "
                                    "for"+str(entry.entryNum)+' '+str(e))
                                return 1

                            # Update the running time for this
                            m = MovFile(entry_temp_file.name)
                            runtimeseconds = int(m.getDuration())
                            # 24 as 24 frames, the significance of
                            # 42 is unknown to me
                            entryTotal = {
                                'sg_entry_run_time': runtimeseconds*24*42
                            }
                            self.sg.update('Version', submitInfo['id'],
                                           entryTotal)

                            # Thumbnail is generated from entry media so
                            # update this as well
                            _entryThumbFilename = '%s.thumb.0720.0404.jpg' % (
                                entryFilename,
                            )

                            entry_thumb_file = tempfile.NamedTemporaryFile(
                                delete=False
                            )
                            f = open(entry_thumb_file.name, 'wb')
                            _, ob_contents = connection.get_object(
                                settings.VES_THUMBS_CONTAINER,
                                _entryThumbFilename,
                                resp_chunk_size=(1024 * 1024 * 40)
                            )

                            for chunk in ob_contents:
                                f.write(chunk)
                            f.close()

                            log("Uploading %s" % _entryThumbFilename)
                            try:
                                self.sg.upload_thumbnail(
                                    "Version", submitInfo['id'],
                                    entry_thumb_file.name)
                                os.unlink(entry_thumb_file.name)
                            except Exception, e:
                                log("Shotgun Upload Error on entry "
                                    "thumbnail for "
                                    ""+str(entry.entryNum)+' '+str(e))

                        else:
                            log_error('Entry file not present after swift '
                                      'download %s ' % entry_mp4_name)
                    else:
                        log("Entry File In Shotgun already %s " % nameInShotgun)

                else:
                    log("Not Present entry MOV for %s - %s" %(entry.entryNum, entryFilename))

        # ---------------------------------
        # if the before and afters media has changed
        # ---------------------------------
        if updateBAMedia:
            log('UPDATING B&A MEDIA FOR '+str(entry.entryNum))
            submitInfo = self.getSubmitInfo(entry)
            if submitInfo is None:  # no entry exists, fail
                log('Failed to find entry in shotgun, '
                    'not uploading b and a media for '+str(entry.entryNum))
                return 1
            else:
                entryFiles = EntryFiles(entry)
                entryFiles.findFiles()
                baMd5 = entryFiles.getUserBaMD5()

                if entryFiles.ba_name:
                    baFilename = os.path.basename(entryFiles.ba_name)
                else:
                    baFilename = None

                if baMd5 is not None and baFilename is not None:

                    ba_mp4_name = '%s.ba.%s.mov.%s.mp4' % (
                        entry.entryNum,
                        baMd5,
                        MAIN_PROXY_NAME)

                    if submitInfo['sg_uploaded_bna']:
                        nameInShotgun = submitInfo['sg_uploaded_bna']['name']
                    else:
                        nameInShotgun = None

                    if not nameInShotgun or not nameInShotgun.endswith(ba_mp4_name):

                        ba_temp_file = tempfile.NamedTemporaryFile(
                            suffix=ba_mp4_name
                        )
                        f = open(ba_temp_file.name, 'wb')
                        _, ob_contents = connection.get_object(
                            settings.VES_PROXY_CONTAINER,
                            ba_mp4_name,
                            resp_chunk_size=(1024 * 1024 * 40))

                        for chunk in ob_contents:
                            f.write(chunk)
                        f.close()

                        if os.path.exists(ba_temp_file.name):
                            log("Uploading %s" % ba_mp4_name)

                            try:
                                self.sg.upload("Version", submitInfo['id'],
                                               ba_temp_file.name,
                                               "sg_uploaded_bna",
                                               ba_mp4_name)
                            except Exception, e:
                                log("Shotgun Upload Error on banda "
                                    "media for"+str(entry.entryNum)+' '+str(e))

                            # Update the running time for this
                            m = MovFile(ba_temp_file.name)
                            baTotal = {
                                'sg_ba_run_time': int(m.getDuration())*24*42
                            }
                            self.sg.update('Version', submitInfo['id'],
                                           baTotal)

                        else:
                            log_error('BA file not present after swift '
                                      'download %s ' % ba_temp_file.name)

                    else:
                        log("BA File In Shotgun already %s " % nameInShotgun)
                else:
                    log("Not Present BA MOV for "+str(entry.entryNum))

        # ---------------------------------
        # if the supp materials have changed
        # ---------------------------------
        if updateSuppMaterials:
            log('Updating supplemental materials for '+str(entry.entryNum))

            submitInfo = self.getSubmitInfo(entry)
            if submitInfo is None:  # no entry exists, fail
                log('Failed to find entry in shotgun, not uploading '
                    'supplementary materials for '+str(entry.entryNum))
                return 1
            else:
                _pdfSupplmentalsFilename = '%s.pdf' % entry.entryNum

                pdf_file = tempfile.NamedTemporaryFile()
                f = open(pdf_file.name, 'wb')
                _, ob_contents = connection.get_object(
                    settings.VES_PDF_CONTAINER,
                    _pdfSupplmentalsFilename,
                    resp_chunk_size=(1024 * 1024 * 40))

                for chunk in ob_contents:
                    f.write(chunk)
                f.close()

                log("Uploading "+_pdfSupplmentalsFilename)
                shotgunName = str(entry.entryNum)+'.pdf'
                try:
                    self.sg.upload("Version", submitInfo['id'],
                                   pdf_file.name,
                                   "sg_supplement_form", shotgunName)
                except Exception, e:
                    log("Shotgun Upload Error on supplementary "
                        "materials for"+str(entry.entryNum)+' '+str(e))
                    return 1

        if updateBAMedia or updateEntryMedia:
            log('Updating run times for '+str(entry.entryNum))
            submitInfo = self.getSubmitInfo(entry)
            entryTotals = self.sg.find_one(
                'Version', [['project', 'is', self.project_info],
                            ['code', 'is',
                             unicode(entry.entryNum).encode(
                                 "ISO-8859-1", 'ignore')]],
                ['sg_entry_run_time', 'sg_ba_run_time', 'sg_total_run_time']
            )
            if entryTotals['sg_entry_run_time'] is not None \
                    and entryTotals['sg_ba_run_time'] is None:
                self.sg.update(
                    'Version', submitInfo['id'],
                    {'sg_total_run_time': entryTotals['sg_entry_run_time']})
            elif entryTotals['sg_entry_run_time'] is None \
                    and entryTotals['sg_ba_run_time'] is not None:
                self.sg.update(
                    'Version', submitInfo['id'],
                    {'sg_total_run_time': entryTotals['sg_ba_run_time']})
            elif entryTotals['sg_entry_run_time'] is not None \
                    and entryTotals['sg_ba_run_time'] is not None:
                self.sg.update(
                    'Version', submitInfo['id'],
                    {'sg_total_run_time': (entryTotals['sg_ba_run_time'] +
                                           entryTotals['sg_entry_run_time'])}
                )

        return 0


def getShotgun():
    shotgun = ShotgunVES(PROJECT_INFO)
    shotgun.connect()
    return shotgun


def testShotgunUpdate():
    if settings.UPDATE_SHOTGUN:
        entry = Entry.objects.get(id=1)
        log('Testing Entry %s ' % entry)
        shotgun = getShotgun()
        shotgun.createUpdateShotgunEntry(entry, True, True, True, True)
    else:
        logger.warning('Not updating Shotgun')


def shotgunUpdate(entry_id, updateEntryDetails=True, updateEntryMedia=True,
                  updateBAMedia=True, updateSuppMaterials=True):
    try:
        if settings.UPDATE_SHOTGUN:
            entry = Entry.objects.get(id=entry_id)
            log('Updating Entry %s ' % entry)
            shotgun = getShotgun()
            shotgun.createUpdateShotgunEntry(
                entry, updateEntryDetails, updateEntryMedia,
                updateBAMedia, updateSuppMaterials)
        else:
            logger.warning('Not updating Shotgun')
    except Exception as e:
        log_exception(e)


def processShotgunUpdate():
    if settings.UPDATE_SHOTGUN:
        entries = Entry.objects.all()
        log('Updating All Entries')
        shotgun = getShotgun()
        for entry in entries:
            try:
                shotgun.createUpdateShotgunEntry(entry, True, False, False, False)
                log('Updated %s' % entry)
            except Exception:
                log('Could not update %s' % entry)
    else:
        logger.warning('Not updating Shotgun')
