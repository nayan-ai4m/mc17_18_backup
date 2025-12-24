# SPDX-License-Identifier: LGPL-2.1-or-later

# Copyright (c) 2019 Red Hat, Inc.
# Copyright (c) 2019 Tomáš Mráz <tmraz@fedoraproject.org>

from subprocess import CalledProcessError, check_output

from .configgenerator import ConfigGenerator

RH_ALLOW_SHA1 = '''
[openssl_init]
alg_section = evp_properties

[evp_properties]
rh-allow-sha1-signatures = yes
'''

FIPS_MODULE_CONFIG = '''
[fips_sect]
tls1-prf-ems-check = {}
activate = 1
'''


class OpenSSLGenerator(ConfigGenerator):
    CONFIG_NAME = 'openssl'
    SCOPES = {'tls', 'ssl', 'openssl'}

    cipher_not_map = {
        'AES-256-CTR': '',
        'AES-128-CTR': '',
        'AES-256-GCM': '-AES256',
        'AES-128-GCM': '-AES128',
        'AES-256-CBC': '-SHA256',
        'AES-128-CBC': '',
        'CHACHA20-POLY1305': '-CHACHA20',
        'SEED-CBC': '-SEED',
        'IDEA-CBC': '!IDEA',
        'DES-CBC': '!DES',
        'RC4-40': '',
        'DES40-CBC': '',
        '3DES-CBC': '-3DES',
        'RC4-128': '!RC4',
        'RC2-CBC': '!RC2',
        'NULL': '!eNULL:!aNULL'
    }

    key_exchange_map = {
        'RSA': 'kRSA',
        'ECDHE': 'kEECDH',
        'PSK': 'kPSK',
        'DHE-PSK': 'kDHEPSK',
        'DHE-RSA': 'kEDH',
        'DHE-DSS': '',
        'ECDHE-PSK': 'kECDHEPSK',
        'RSA-PSK': 'kRSAPSK',
        'VKO-GOST-2012': 'kGOST'
    }

    key_exchange_not_map = {
        'ANON': '',
        'DH': '',
        'ECDH': '',
        'RSA': '-kRSA',
        'ECDHE': '-kEECDH',
        'DHE-RSA': '-aRSA',
        'DHE-DSS': '-aDSS',
        'PSK': '-kPSK',
        'DHE-PSK': '-kDHEPSK',
        'ECDHE-PSK': '-kECDHEPSK',
        'RSA-PSK': '-kRSAPSK'
    }

    mac_not_map = {
        'HMAC-MD5': '!MD5',
        'HMAC-SHA1': '-SHA1'
    }

    ciphersuite_map = {
        'AES-256-GCM': 'TLS_AES_256_GCM_SHA384',
        'AES-128-GCM': 'TLS_AES_128_GCM_SHA256',
        'CHACHA20-POLY1305': 'TLS_CHACHA20_POLY1305_SHA256',
        'AES-128-CCM': 'TLS_AES_128_CCM_SHA256',
        'AES-128-CCM8': 'TLS_AES_128_CCM_8_SHA256',
        'GOST28147-TC26Z-CNT': 'GOST2012-GOST8912-GOST8912',
        'GOST28147-CPA-CNT': 'GOST2001-GOST89-GOST89'
    }

    @classmethod
    def generate_ciphers(cls, policy):
        s = ''
        p = policy.enabled
        ip = policy.disabled
        # We cannot separate RSA strength from DH params.
        min_dh_size = policy.integers['min_dh_size']
        min_rsa_size = policy.integers['min_rsa_size']
        if min_dh_size < 1023 or min_rsa_size < 1023:
            s = cls.append(s, '@SECLEVEL=0')
        elif min_dh_size < 2048 or min_rsa_size < 2048:
            s = cls.append(s, '@SECLEVEL=1')
        elif min_dh_size < 3072 or min_rsa_size < 3072:
            s = cls.append(s, '@SECLEVEL=2')
        else:
            s = cls.append(s, '@SECLEVEL=3')

        for i in p['key_exchange']:
            try:
                s = cls.append(s, cls.key_exchange_map[i])
            except KeyError:
                pass

        for i in ip['key_exchange']:
            try:
                s = cls.append(s, cls.key_exchange_not_map[i])
            except KeyError:
                pass

        for i in ip['cipher']:
            try:
                s = cls.append(s, cls.cipher_not_map[i])
            except KeyError:
                pass
        if 'AES-128-CCM' in ip['cipher']:
            if 'AES-256-CCM' in ip['cipher']:
                s = cls.append(s, '-AESCCM')

        for i in ip['mac']:
            try:
                s = cls.append(s, cls.mac_not_map[i])
            except KeyError:
                pass

        # These ciphers are not necessary for any
        # policy level, and only increase the attack surface.
        # FIXME! must be fixed for custom policies
        for c in ('-SHA384', '-CAMELLIA', '-ARIA', '-AESCCM8'):
            s = cls.append(s, c)

        return s

    @classmethod
    def generate_ciphersuites(cls, policy):
        s = ''
        p = policy.enabled
        for i in p['cipher']:
            try:
                s = cls.append(s, cls.ciphersuite_map[i])
            except KeyError:
                pass

        return s

    @classmethod
    def generate_config(cls, policy):
        return cls.generate_ciphers(policy) + '\n'

    @classmethod
    def test_config(cls, config):
        output = b''
        assert config.endswith('\n')  # noqa: S101
        try:
            output = check_output(['openssl',  # noqa: S607
                                   'ciphers', config[:-1]])
        except CalledProcessError:
            cls.eprint('There is an error in openssl generated policy')
            cls.eprint(f'Policy:\n{config}')
            return False
        except OSError:
            # Ignore missing openssl
            return True
        if b'NULL' in output or b'ADH' in output:
            cls.eprint('There is NULL or ADH in openssl generated policy')
            cls.eprint(f'Policy:\n{config}')
            return False
        return True


class OpenSSLConfigGenerator(OpenSSLGenerator):
    CONFIG_NAME = 'opensslcnf'

    # has to cover everything c-p has
    protocol_map = {
        'SSL3.0': 'SSLv3',
        'TLS1.0': 'TLSv1',
        'TLS1.1': 'TLSv1.1',
        'TLS1.2': 'TLSv1.2',
        'TLS1.3': 'TLSv1.3',
        'DTLS0.9': 'DTLSv0.9',
        'DTLS1.0': 'DTLSv1',
        'DTLS1.2': 'DTLSv1.2'
    }

    sign_map = {
        'RSA-SHA1': 'RSA+SHA1',
        'DSA-SHA1': 'DSA+SHA1',
        'ECDSA-SHA1': 'ECDSA+SHA1',
        'RSA-SHA2-224': 'RSA+SHA224',
        'DSA-SHA2-224': 'DSA+SHA224',
        'ECDSA-SHA2-224': 'ECDSA+SHA224',
        'RSA-SHA2-256': 'RSA+SHA256',
        'DSA-SHA2-256': 'DSA+SHA256',
        'ECDSA-SHA2-256': 'ECDSA+SHA256',
        'RSA-SHA2-384': 'RSA+SHA384',
        'DSA-SHA2-384': 'DSA+SHA384',
        'ECDSA-SHA2-384': 'ECDSA+SHA384',
        'RSA-SHA2-512': 'RSA+SHA512',
        'DSA-SHA2-512': 'DSA+SHA512',
        'ECDSA-SHA2-512': 'ECDSA+SHA512',
        'RSA-PSS-SHA2-256': 'rsa_pss_pss_sha256',
        'RSA-PSS-SHA2-384': 'rsa_pss_pss_sha384',
        'RSA-PSS-SHA2-512': 'rsa_pss_pss_sha512',
        'RSA-PSS-RSAE-SHA2-256': 'rsa_pss_rsae_sha256',
        'RSA-PSS-RSAE-SHA2-384': 'rsa_pss_rsae_sha384',
        'RSA-PSS-RSAE-SHA2-512': 'rsa_pss_rsae_sha512',
        'EDDSA-ED25519': 'ed25519',
        'EDDSA-ED448': 'ed448',
    }

    group_map = {
        'SECP224R1': 'secp224r1',
        'SECP256R1': 'secp256r1',
        'SECP384R1': 'secp384r1',
        'SECP521R1': 'secp521r1',
        'X25519': 'X25519',
        'X448': 'X448',
        'FFDHE-2048': 'ffdhe2048',
        'FFDHE-3072': 'ffdhe3072',
        'FFDHE-4096': 'ffdhe4096',
        'FFDHE-6144': 'ffdhe6144',
        'FFDHE-8192': 'ffdhe8192',
        'BRAINPOOL-P256R1': 'brainpoolP256r1',
        'BRAINPOOL-P384R1': 'brainpoolP384r1',
        'BRAINPOOL-P512R1': 'brainpoolP512r1',
    }

    @classmethod
    def generate_config(cls, policy):
        p = policy.enabled
        # This includes the seclevel
        s = f'CipherString = {cls.generate_ciphers(policy)}\n'
        s += f'Ciphersuites = {cls.generate_ciphersuites(policy)}\n'

        if policy.min_tls_version:
            s += 'TLS.MinProtocol ='
            s += f' {cls.protocol_map[policy.min_tls_version]}\n'
        if policy.max_tls_version:
            s += 'TLS.MaxProtocol ='
            s += f' {cls.protocol_map[policy.max_tls_version]}\n'
        if policy.min_dtls_version:
            s += 'DTLS.MinProtocol ='
            s += f' {cls.protocol_map[policy.min_dtls_version]}\n'
        if policy.max_dtls_version:
            s += 'DTLS.MaxProtocol ='
            s += f' {cls.protocol_map[policy.max_dtls_version]}\n'

        sig_algs = [cls.sign_map[i] for i in p['sign'] if i in cls.sign_map]
        s += 'SignatureAlgorithms = ' + ':'.join(sig_algs) + '\n'

        groups = [cls.group_map[i] for i in p['group'] if i in cls.group_map]
        s += 'Groups = ' + ':'.join(groups) + '\n'

        if policy.enums['__ems'] == 'RELAX':
            s += 'Options = RHNoEnforceEMSinFIPS\n'

        if 'SHA1' in p['hash']:
            s += RH_ALLOW_SHA1

        return s

    @classmethod
    def test_config(cls, config):  # pylint: disable=unused-argument
        return True


class OpenSSLFIPSGenerator(ConfigGenerator):
    CONFIG_NAME = 'openssl_fips'
    SCOPES = {'tls', 'ssl', 'openssl'}

    @classmethod
    def generate_config(cls, policy):
        # OpenSSL EMS relaxation is special
        # in that it uses a separate FIPS module config
        # and, just in case, EMS is enforcing by default.
        # It only puts `= 0` there if it's explicitly relaxed.
        # That's the reason why `__ems` is a tri-state enum.
        return FIPS_MODULE_CONFIG.format(int(policy.enums['__ems'] != 'RELAX'))

    @classmethod
    def test_config(cls, config):  # pylint: disable=unused-argument
        return True
