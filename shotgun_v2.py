import logging
import os
import re
import tempfile
import utils

from applications.models import ApplicationStorageLocation
from django.conf import settings
from pprint import pformat
from shotgun_api3 import Shotgun, ShotgunError
from sohonet_encode.movtool import MovFile
from swiftclient import ClientException

from ves.awards.models import EntryFiles
from ves.awards.views import genSlate

SERVER_PATH = 'https://ves.shotgunstudio.com'
MAIN_PROXY_NAME = settings.VES_MAIN_PROXY_NAME


class ShotgunVES(Shotgun):

    shotgun_submission_entity = 'CustomEntity04'
    shotgun_version_entity = 'Version'

    def __init__(self, **kwargs):

        self.logger = logging.getLogger(__name__)

        _verbose = kwargs.get('verbose', False)

        if _verbose:
            self.level = logging.DEBUG
        else:
            self.level = logging.INFO

        self.logger.info('ShotgunVES Starting.')

        self.project_info = {
            'type': 'Project', 'id': settings.SHOTGUN_PROJECT_ID
        }
        self._project_filter = [
            'project', 'is', self.project_info
        ]

        super(ShotgunVES, self).__init__(
            SERVER_PATH, settings.SHOTGUN_USER, settings.SHOTGUN_KEY
        )

    def exception(self, msg):
        self.logger.exception(msg)

    def error(self, msg):
        self.logger.error(msg)

    def debug(self, msg):
        self.logger.debug(msg)

    def log(self, msg):
        self.logger.log(self.level, msg)

    def _find_in_project(self, entity_type, filters, *args, **kwargs):
        filters.append(self._project_filter)
        return self.find_one(
            entity_type, filters, *args, **kwargs
        )

    def get_category(self, category_number):
        category_fields = ['code', 'sg_category_number']
        category = self._find_in_project(
            'Shot', [['sg_category_number', 'is', category_number]],
            category_fields
        )
        if not category:
            raise Exception(
                'Could not find shotgun category %s' % category_number
            )
        return category

    def retire_entry(self, entry_num):
        """
        Retires an entry, can be reversed under most circumstances
        see the API documentation
        :param entry_num:
        :return:
        """
        entry = self._find_in_project(
            self.shotgun_submission_entity,
            [['code', 'is', entry_num]],
            ['id', 'code']
        )
        if entry is None:
            return None
        else:
            # Set entry as withdrawn
            self.update(
                self.shotgun_submission_entity,
                entry['id'],
                {'sg_status_list': 'wdraw'}
            )
            return entry['id']

    def get_vetting_check_list(self):
        """
        get some default vetting fields to attach for new entries.
        :return: vetting check list
        """
        vetting_list = self.find_one(
            "TaskTemplate",
            [['code', 'is', 'vettingCheckList']]
        )
        if not vetting_list:
            raise Exception('Could not find vetting list.')
        return vetting_list

    def update_entry_status(self, entry):

        if entry.shotgunSync:
            self.log('Entry %s is marked to not update shotgun '
                     'Entry.shotgunSync ' % entry.entryNum)
            return 1

        if entry.hasBeenDeleted:
            self.log(
                'Entry %s has been deleted, retiring from shotgun '
                % str(entry.entryNum)
            )
            retire_result = self.retire_entry(entry.entry_num)
            if retire_result:
                self.log('Retired %s from shotgun ' % entry.entryNum)
            else:
                self.exception(
                    'Could not retire %s from shotgun ' % entry.entryNum
                )
            return 1

        self.log('Entry %s status is up to date.' % entry.entryNum)

    def get_user(self, user_data):
        """
        Returns a user from the data, either an existing one or one that it
        creates in this method
        :param user_data:
        :return:
        """

        if not user_data['firstname'] and not user_data['lastname']:
            return None

        _name_lower = "%s_%s" % (
            user_data['firstname'].lower(), user_data['lastname'].lower()
        )

        user_login = re.sub(r'\s+', '', _name_lower)
        user_login = user_login.replace('.', '').replace('-', '')

        # Shotgun needs to have non unicode characters or else is replaces
        # then with a space
        user_login = user_login.encode('ascii', errors='ignore')

        user_data['login'] = user_login
        user_info = self.find_one(
            'HumanUser',
            [['login', 'is', user_login]], ['groups', 'projects']
        )

        if user_info is None:
            user_data['login'] = user_login
            user_data['projects'] = [self.project_info]
            user_data['sg_status_list'] = 'dis'
            user_data = self.create('HumanUser', user_data)
            return user_data
        else:
            # Add New Projects
            project_list = [self.project_info]
            this_project_info = (user_info['projects'])
            for project in this_project_info:
                project_list.extend(
                    [{'type': 'Project', 'id': project['id']}]
                )
            user_data['projects'] = project_list

            # Update User
            user_data = self.update('HumanUser', user_info['id'], user_data)
            return user_data

    # Generate a VES shotgun specific list of entrant dictionaries from a
    # entry object
    def generate_entrant_data(self, entry):

        # a list of dictionary's, containing entrant information
        entrant_list = []
        entrants = []
        entrant_dict = {
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

        for entrant, model_identifier in entrants:
            job_title_or_credit = getattr(
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
            country = str(entrant.country).encode("ISO-8859-1", 'ignore')
            entrant_data = {
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
                'sg_job_title': job_title_or_credit,
                'sg_state': utils.get_clean_state(entrant.stateProvince),
                'sg_phone': entrant.primaryPhone,
                'sg_entrant_number': model_identifier,
                'sg_credit_url': url_data,
            }
            entrant_info = self.get_user(entrant_data)

            if entrant_info is not None:
                entrant_list.extend(
                    [{'type': 'HumanUser', 'id': entrant_info['id']}])

            entrant_dict['entrant_%s' % model_identifier] = {
                'type': 'HumanUser', 'id': entrant_info['id']
            }
            _jt_index = 'entrant_%s_job_title' % model_identifier
            _url_index = 'entrant_%s_url' % model_identifier

            entrant_dict[_jt_index] = job_title_or_credit
            entrant_dict[_url_index] = url_data

        return entrant_list, entrant_dict

    def generate_signature_data(self, entry):
        """
        Generate a VES shotgun specific list of signature dictionaries from a
        sohonet entry object
        :param entry:
        :return:
        """

        signature_details_list = []
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
            job_title_or_credit = getattr(
                entry, 'e%sjobTitleOrCredit' % e_id, ''
            )
            sg_country = str(entrant.country).encode("ISO-8859-1", 'ignore')
            signature_data = {
                'sg_country': sg_country,
                'email': entrant.emailAddr,
                'firstname': entrant.firstName,
                'lastname': entrant.lastName,
                'sg_fax': entrant.fax,
                'sg_phone': entrant.primaryPhone,
                'sg_job_title': job_title_or_credit,
                'sg_signature_number': sg_sig_number,
            }
            signature_info = self.get_user(signature_data)
            if signature_info is not None:
                signature_details_list.extend(
                    [{'type': 'HumanUser', 'id': signature_info['id']}]
                )

        return signature_details_list

    def generate_contact_data(self, entry):
        """
        Generate a VES shotgun specific 'contact' dictionary from a
        sohonet entry object
        :param entry:
        :return:
        """

        country = str(
            entry.submissionContact.country
        ).encode("ISO-8859-1", 'ignore')

        state = utils.get_clean_state(entry.submissionContact.stateProvince)

        contact_data = {
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
            'sg_job_title': '',
            'sg_state': state,
            'sg_phone': entry.submissionContact.primaryPhone,
        }
        contact_info = self.get_user(contact_data)
        contact_data = {'type': 'HumanUser', 'id': contact_info['id']}

        return contact_data

    @staticmethod
    def get_version_fields():
        return [
            'id', 'image', 'sg_supplement_form', 'sg_entry_slate',
            'sg_bna_slate', 'sg_uploaded_movie', 'sg_uploaded_bna'
        ]

    def get_submit_info(self, entry):
        """
        return a current submission if it exists, otherwise return None
        :param entry:
        :return:
        """
        return self._find_in_project(
            self.shotgun_submission_entity,
            [
                ['code', 'is', str(entry.entryNum)],
            ]
        )

    def get_company(self, company):
        """
        See if a company is already in the database,
        if so returns the identifiers
        :param company:
        :return:
        """
        data = self.find_one(
            'CustomNonProjectEntity01', [['code', 'is', company]], ['id']
        )
        if data and company != '':
            data = self.create(
                'CustomNonProjectEntity01', {'code': company}
            )
        if data:
            return {'type': 'CustomNonProjectEntity01', 'id': data['id']}
        else:
            return None

    def generate_submission_data(self, entry, vetting_list,
                                 entrant_details_list,
                                 entrant_dict, signature_details_list,
                                 contact_data, category):
        """
        Generate a VES shotgun specific submit dictionary, this dictionary is
        then the main object to create a submission in shotgun.
        :param entry:
        :param vetting_list:
        :param entrant_details_list:
        :param entrant_dict:
        :param signature_details_list:
        :param contact_data:
        :param category:
        :return:
        """

        distribution_company = self.get_company(entry.distributionCompany)
        production_company = self.get_company(entry.productionCompany)

        submit_data = {
            'code': str(entry.entryNum),
            'sg_entry_title': entry.sequenceOrShotname,
            'sg_project_title': entry.projectName,
            'sg_premiere_date': entry.dateOfPremiere.strftime('%Y-%m-%d'),
            'project': self.project_info,
            'sg_category': category,
            'sg_production_company': production_company,
            'sg_terms_aggred': True,
            'sg_facility_employed': entry.entryAtFacility,
            'sg_submitter_list': entrant_details_list,
            'sg_signature_list': signature_details_list,
            'sg_contact': contact_data,
            'sg_time_to_screen_submissions': 600000,
            'sg_time_to_vote': 300000,
            'sg_time_to_read_suppliments': 90000,
            'task_template': vetting_list,
            'sg_soho_updated': entry.lastEdit,
            'sg_payment': entry.hasPaid,
            'sg_payment_amount': float(entry.getPrice()),
            'sg_submitter_1': entrant_dict['entrant_1'],
            'sg_submitter_1_url': entrant_dict['entrant_1_url'],
            'sg_submitter_1_job_title': entrant_dict['entrant_1_job_title'],
            'sg_submitter_2': entrant_dict['entrant_2'],
            'sg_submitter_2_url': entrant_dict['entrant_2_url'],
            'sg_submitter_2_job_title': entrant_dict['entrant_2_job_title'],
            'sg_submitter_3': entrant_dict['entrant_3'],
            'sg_submitter_3_url': entrant_dict['entrant_3_url'],
            'sg_submitter_3_job_title': entrant_dict['entrant_3_job_title'],
            'sg_submitter_4': entrant_dict['entrant_4'],
            'sg_submitter_4_url': entrant_dict['entrant_4_url'],
            'sg_submitter_4_job_title': entrant_dict['entrant_4_job_title'],
            'sg_submitter_5': entrant_dict['entrant_5'],
            'sg_submitter_5_url': entrant_dict['entrant_5_url'],
            'sg_submitter_5_job_title': entrant_dict['entrant_5_job_title']

            # TODO - No longer exists in shotgun
            # 'sg_distribution_company': distribution_company,
            # 'sg_entry_mos': entry.no_audio,
            # 'sg_bna_mos': entry.no_audio_ba,
            # 'sg_petition': False,
        }

        return submit_data

    @staticmethod
    def get_connection():
        # TODO - Should just be one storage location
        # TODO - find a better way of getting this.
        storage = ApplicationStorageLocation.objects.latest('created')
        storage.ensure_container(settings.VES_PDF_CONTAINER)
        return storage.get_connection()

    def update_entry_details(self, entry):
        self.log('Updating entry %s details' % entry)

        if self.update_entry_status(entry):
            # Entry has been deleted or marked as do not continue
            return

        category = self.get_category(entry.entryNum.category.catNum)
        vetting_list = self.get_vetting_check_list()

        entrant_details, entrant_list = self.generate_entrant_data(entry)

        self.debug("Entrant Details:")
        self.debug(pformat(entrant_details))

        self.debug("Entrants List:")
        self.debug(pformat(entrant_list))

        signature_details_list = self.generate_signature_data(entry)

        contact_data = self.generate_contact_data(entry)

        self.debug("Contact Data:")
        self.debug(pformat(contact_data))

        submit_info = self.get_submit_info(entry)

        self.debug("Submit Info:")
        self.debug(pformat(submit_info))

        submit_data = self.generate_submission_data(
            entry, vetting_list, entrant_details, entrant_list,
            signature_details_list, contact_data, category
        )

        if submit_info is None:  # entry is not in shotgun yet
            # need to create new
            self.log('Creating new submission %s' % entry.entryNum)
            submit_data = self.create(self.shotgun_submission_entity, submit_data)
        else:
            # entry is in shotgun,
            # so need to update the existing entry
            self.log('Updating submission ' + str(entry.entryNum))
            submit_data = self.update(
                self.shotgun_submission_entity, submit_info['id'], submit_data
            )

        self.log("Uploading entry Slate")
        # text on the slates may have changed so update these
        temp_dir = tempfile.mkdtemp()
        aa_slate_contents = genSlate(entry.id, True)
        aa_shotgun_name = str(entry.entryNum) + '.slateEntry.png'
        aa_temp_file = os.path.join(temp_dir, aa_shotgun_name)
        aa_temp_file_h = open(aa_temp_file, 'wb')
        aa_temp_file_h.write(aa_slate_contents)
        aa_temp_file_h.close()

        try:
            self.upload(
                self.shotgun_submission_entity,
                submit_data['id'],
                aa_temp_file,
                "sg_entry_slate",
                aa_shotgun_name
            )
            os.unlink(aa_temp_file)
            os.rmdir(temp_dir)

        except ShotgunError:

            self.exception(
                "Shotgun Upload Error on entry slate for %s " % entry.entryNum
            )

        self.log("Uploading banda Slate")

        temp_dir = tempfile.mkdtemp()
        ba_slate_contents = genSlate(entry.id, False)
        ba_shotgun_name = str(entry.entryNum) + '.slateBNA.png'
        ba_temp_file = os.path.join(temp_dir, ba_shotgun_name)
        ba_temp_file_h = open(ba_temp_file, 'wb')
        ba_temp_file_h.write(ba_slate_contents)
        ba_temp_file_h.close()

        try:
            self.upload(
                self.shotgun_submission_entity,
                submit_data['id'],
                ba_temp_file_h.name,
                "sg_bna_slate",
                ba_shotgun_name
            )
            os.unlink(ba_temp_file)
            os.rmdir(temp_dir)
        except ShotgunError:
            self.exception(
                "Shotgun Upload Error on banda slate for %s" % entry.entryNum
            )

    def get_version_info(self, code):
        """
        return a current submission if it exists, otherwise return None
        :param code:
        :return:
        """
        return self._find_in_project(
            self.shotgun_version_entity,
            [
                ['code', 'is', code],
            ],
            self.get_version_fields()
        )

    def update_ba_media(self, entry):
        self._update_media(entry, False)

    def update_entry_media(self, entry):
        self._update_media(entry, True)

    def _update_media(self, entry, aa):
        self.log('Updating %s media ' % entry.entryNum)

        submit_info = self.get_submit_info(entry)

        if submit_info is None:  # no entry exists, fail
            self.log(
                'Failed to find entry %s details in shotgun, '
                'not uploading entry media.' % str(entry.entryNum)
            )
            return 1

        entry_files = EntryFiles(entry)
        entry_files.findFiles()

        if aa and not entry_files.entry_found:
            self.log(
                "AA Not Found in swift %s - %s"
                % (entry.entryNum, entry)
            )
            return

        if not aa and not entry_files.ba_found:
            self.log(
                "BA Not Found in swift %s - %s"
                % (entry.entryNum, entry)
            )
            return

        if aa:
            entry_md5 = entry_files.getUserEntryMD5()
        else:
            entry_md5 = entry_files.getUserBaMD5()

        if entry_files.entry_name:
            entry_filename = os.path.basename(entry_files.entry_name)
        else:
            entry_filename = None

        if entry_md5 is not None and entry_filename is not None:
            if aa:
                entry_mp4_name = entry.aa_code(entry_md5)
                swift_mp4_name = '%s.aa.%s.mov.%s.mp4' % (
                    entry.entryNum,
                    entry_md5,
                    MAIN_PROXY_NAME
                )
                code = entry.aa_code(entry_md5)
            else:
                entry_mp4_name = entry.ba_code(entry_md5)
                swift_mp4_name = '%s.ba.%s.mov.%s.mp4' % (
                    entry.entryNum,
                    entry_md5,
                    MAIN_PROXY_NAME
                )
                code = entry.ba_code(entry_md5)

            version_info = self.get_version_info(entry_mp4_name)
            if version_info is None:  # no version exists, create
                self.log(
                    'Failed to find version %s in shotgun, '
                    'creating new version.' % entry.entryNum
                )
                version_info = self.create(
                    self.shotgun_version_entity,
                    {
                        'code': code,
                        'entity': {
                            'type': self.shotgun_submission_entity,
                            'id': submit_info['id']
                        },
                        'project': self.project_info
                    },
                    self.get_version_fields()
                )

            sg_uploaded_movie = version_info['sg_uploaded_movie']

            requires_upload = False
            if not sg_uploaded_movie:
                requires_upload = True
            else:
                if sg_uploaded_movie.get('name') != entry_mp4_name:
                    requires_upload = True

            if requires_upload:
                self.log(
                    "Shotgun field sg_uploaded_movie: %s" % sg_uploaded_movie
                )
                temp_dir = tempfile.mkdtemp()
                entry_temp_file = os.path.join(temp_dir, entry_mp4_name)

                f = open(entry_temp_file, 'wb')

                connection = self.get_connection()
                _, ob_contents = connection.get_object(
                    settings.VES_PROXY_CONTAINER,
                    swift_mp4_name,
                    resp_chunk_size=(1024 * 1024 * 40)
                )

                for chunk in ob_contents:
                    f.write(chunk)
                f.close()

                self.log("Uploading %s " % entry_mp4_name)
                try:
                    self.upload(
                        self.shotgun_version_entity,
                        version_info['id'],
                        entry_temp_file,
                        "sg_uploaded_movie",
                        entry_mp4_name,
                        entry_mp4_name
                    )
                except ShotgunError:
                    self.exception(
                        "Shotgun Upload Error on "
                        "entry media %s" + entry.entryNum
                    )
                    return 1

                # Update the running time for this
                m = MovFile(entry_temp_file)
                runtime_seconds = int(m.getDuration())

                # 24 as 24 frames, the significance of
                # 42 is unknown to me
                entry_total = {
                    'sg_entry_run_time': runtime_seconds * 24 * 42
                }
                self.update(
                    self.shotgun_version_entity,
                    version_info['id'],
                    entry_total
                )

                os.unlink(entry_temp_file)
                os.rmdir(temp_dir)

                # Thumbnail is generated from entry media so
                # update this as well
                _entryThumbFilename = '%s.thumb.0720.0404.jpg' % (
                    entry_filename,
                )

                thumb_temp_dir = tempfile.mkdtemp()

                entry_thumb_file = os.path.join(
                    thumb_temp_dir, _entryThumbFilename
                )

                f = open(entry_thumb_file, 'wb')
                _, ob_contents = connection.get_object(
                    settings.VES_THUMBS_CONTAINER,
                    _entryThumbFilename,
                    resp_chunk_size=(1024 * 1024 * 40)
                )

                for chunk in ob_contents:
                    f.write(chunk)
                f.close()

                self.log("Uploading %s" % _entryThumbFilename)
                try:
                    self.upload_thumbnail(
                        self.shotgun_version_entity,
                        version_info['id'],
                        entry_thumb_file
                    )
                    os.unlink(entry_thumb_file)
                    os.rmdir(thumb_temp_dir)
                except ShotgunError:
                    self.exception(
                        "Shotgun Upload Error on entry thumbnail %s "
                        % str(entry.entryNum)
                    )
            else:
                self.log(
                    "Version %s exists in shotgun, not uploading"
                    % sg_uploaded_movie['name']
                )
        else:
            self.log(
                "Not Present entry MOV for %s - %s"
                % (entry.entryNum, entry_filename)
            )

        self.update_run_times(entry)

    def update_run_times(self, entry):
        self.log('Updating %s run times ' % entry.entryNum)

        entry_totals = self._find_in_project(
            self.shotgun_submission_entity,
            [['code', 'is', str(entry.entryNum)]],
            ['sg_entry_run_time', 'sg_ba_run_time', 'sg_total_run_time']
        )

        et_entry_runtime = entry_totals['sg_entry_run_time']
        et_ba_runtime = entry_totals['sg_ba_run_time']

        if et_entry_runtime is not None and et_ba_runtime is None:
            self.update(
                self.shotgun_submission_entity, entry_totals['id'],
                {'sg_total_run_time': et_entry_runtime}
            )
        elif et_entry_runtime is None and et_ba_runtime is not None:
            self.update(
                self.shotgun_submission_entity, entry_totals['id'],
                {'sg_total_run_time': et_ba_runtime}
            )
        elif et_entry_runtime is not None and et_ba_runtime is not None:
            self.update(
                self.shotgun_submission_entity, entry_totals['id'],
                {'sg_total_run_time': (et_entry_runtime + et_ba_runtime)}
            )

    def update_supplemental(self, entry):
        self.log('Updating supplemental materials for ' + str(entry.entryNum))

        submit_info = self.get_submit_info(entry)
        if submit_info is None:  # no entry exists, fail
            self.log(
                'Failed to find entry %s in shotgun, not uploading '
                'supplementary materials for ' % entry.entryNum
            )
            return 1

        self.log("Submit Info:")
        self.log(pformat(submit_info))

        version_info = self.get_version_info(entry.supplemental_code())
        if version_info is not None:  # no version exists, create
            self.log(
                'Supplemental %s exists in shotgun.'
                % entry.entryNum
            )
            return

        self.log(
            'Failed to find supplemental version %s in shotgun, '
            'creating new version.' % entry.entryNum
        )
        version_info = self.create(
            self.shotgun_version_entity,
            {
                'code': entry.supplemental_code(),
                'entity': {
                    'type': self.shotgun_submission_entity,
                    'id': submit_info['id']
                },
                'project': self.project_info
            },
        )

        self.log("Version Info:")
        self.log(pformat(version_info))

        _pdf_supplemental_filename = entry.supplemental_code()

        try:
            connection = self.get_connection()
            try:
                connection.head_object(
                    settings.VES_PDF_CONTAINER,
                    _pdf_supplemental_filename,
                )
                _, ob_contents = connection.get_object(
                    settings.VES_PDF_CONTAINER,
                    _pdf_supplemental_filename,
                    resp_chunk_size=(1024 * 1024 * 40)
                )

                temp_dir = tempfile.mkdtemp()
                pdf_file = os.path.join(temp_dir, entry.supplemental_code())

                f = open(pdf_file, 'wb')

                for chunk in ob_contents:
                    f.write(chunk)
                f.close()

                self.log("Uploading " + _pdf_supplemental_filename)

                self.upload(
                    self.shotgun_version_entity,
                    version_info['id'],
                    pdf_file,
                    "sg_uploaded_movie",
                    entry.supplemental_code()
                )

                self.log(
                    "PDF %s uploaded to shotgun."
                    % _pdf_supplemental_filename
                )

                os.unlink(pdf_file)
                os.rmdir(temp_dir)

            except ClientException as e:
                if e.http_status == 404:
                    self.log(
                        "PDF %s not found in swift"
                        % _pdf_supplemental_filename
                    )
                else:
                    raise e

        except ShotgunError:
            self.exception(
                "Shotgun Upload Error on supplementary materials %s"
                % entry.entryNum
            )
            return 1
