# encoding=utf-8
import abc
import contextlib
import gettext
import logging
import robotexclusionrulesparser
import urllib.parse

from wpull.database import Status
from wpull.errors import (ProtocolError, ServerError, ConnectionRefused,
    DNSNotFound)
from wpull.http import Request
from wpull.stats import Statistics
from wpull.url import URLInfo
from wpull.waiter import LinearWaiter


_logger = logging.getLogger(__name__)
_ = gettext.gettext
REDIRECT_STATUS_CODES = (301, 302, 303, 307, 308)


class BaseProcessor(object, metaclass=abc.ABCMeta):
    @contextlib.contextmanager
    @abc.abstractmethod
    def session(self):
        pass

    @abc.abstractproperty
    def statistics(self):
        pass

    def close(self):
        pass


class BaseProcessorSession(object, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def new_request(self, url_record, url_info):
        pass

    @abc.abstractmethod
    def accept_response(self, response, error=None):
        pass

    @abc.abstractmethod
    def wait_time(self):
        pass

    @abc.abstractmethod
    def get_inline_urls(self):
        pass

    @abc.abstractmethod
    def get_linked_urls(self):
        pass


class WebProcessor(BaseProcessor):
    def __init__(self, url_filters=None, document_scrapers=None,
    file_writer=None, waiter=None, statistics=None, request_factory=None,
    retry_connrefused=False, retry_dns_error=False, max_redirects=20,
    robots=False):
        self._url_filters = url_filters or ()
        self._document_scrapers = document_scrapers or ()
        self._file_writer = file_writer
        self._waiter = waiter or LinearWaiter()
        self._statistics = statistics or Statistics()
        self._request_factory = request_factory or Request.new
        self._retry_connrefused = retry_connrefused
        self._retry_dns_error = retry_dns_error
        self._max_redirects = max_redirects

        if robots:
            self._robots_txt_pool = RobotsTxtPool()
        else:
            self._robots_txt_pool = None

        self._statistics.start()

    @contextlib.contextmanager
    def session(self):
        session = WebProcessorSession(
            self._url_filters,
            self._document_scrapers,
            self._file_writer,
            self._waiter,
            self._statistics,
            self._request_factory,
            self._retry_connrefused,
            self._retry_dns_error,
            self._max_redirects,
            self._robots_txt_pool,
        )
        yield session

    @property
    def statistics(self):
        return self._statistics

    def close(self):
        self._statistics.stop()


class RobotsTxtNotLoaded(Exception):
    pass


class RobotsTxtSubsession(object):
    class Result(object):
        done = 1
        redirect = 2
        retry = 3
        fail = 4

    def __init__(self, robots_txt_pool, request_factory):
        self._robots_txt_pool = robots_txt_pool
        self._request = None
        self._attempts_remaining = 20
        self._redirects_remaining = 5
        self._redirect_url = None
        self._request_factory = request_factory

    def can_fetch(self, request):
        if not self._robots_txt_pool.has_parser(request):
            raise RobotsTxtNotLoaded()

        if not self._robots_txt_pool.can_fetch(request):
            _logger.debug('Cannot fetch {url} due to robots.txt'.format(
                url=request.url_info.url))
            return False
        else:
            return True

    def rewrite_request(self, request):
        url_info = request.url_info

        if self._redirect_url:
            self._request = self._request_factory(self._redirect_url)
            self._redirect_url = None
        else:
            url = URLInfo.parse('{0}://{1}:{2}/robots.txt'.format(
                url_info.scheme, url_info.hostname, url_info.port)).url
            self._request = self._request_factory(url)

        return self._request

    def check_response(self, response):
        if self._attempts_remaining == 0:
            _logger.warn(_('Too many failed attempts to get robots.txt.'))
            return self.Result.fail

        if self._redirects_remaining == 0:
            _logger.warn(_('Ignoring robots.txt redirect loop.'))
            return self.Result.done

        if not response or 500 <= response.status_code <= 599:
            self._attempts_remaining -= 1
            return self.Result.retry
        elif response.status_code in REDIRECT_STATUS_CODES:
            self._accept_empty(self._request.url_info)
            self._redirects_remaining -= 1
            redirect_url = response.fields.get('location')
            return self._parse_redirect_url(redirect_url)
        else:
            self._attempts_remaining = 20
            self._redirects_remaining = 5
            if response.status_code == 200:
                self._accept_ok(response)
                return self.Result.done
            else:
                self._accept_empty(self._request.url_info)
                return self.Result.done

    def _accept_ok(self, response):
        try:
            self._robots_txt_pool.load_robots_txt(self._request.url_info,
                response.body.content_segment())
        except ValueError:
            _logger.warning(_('Failed to parse {url}. Ignoring.').format(
                self._request.url_info.url))
            self._accept_empty(self._request.url_info)

    def _accept_empty(self, url_info):
        self._robots_txt_pool.load_robots_txt(url_info, '')

    def _parse_redirect_url(self, url):
        if url:
            self._redirect_url = url
            return self.Result.redirect

        self._accept_empty(self._request.url_info)
        return self.Result.done


class WebProcessorSession(BaseProcessorSession):
    class State(object):
        normal = 1
        robotstxt = 2

    def __init__(self, url_filters, document_scrapers, file_writer, waiter,
    statistics, request_factory, retry_connrefused, retry_dns_error,
    max_redirects, robots_txt_pool):
        self._url_filters = url_filters
        self._document_scrapers = document_scrapers
        self._file_writer = file_writer
        self._request = None
        self._inline_urls = set()
        self._linked_urls = set()
        self._redirect_url = None
        self._waiter = waiter
        self._statistics = statistics
        self._request_factory = request_factory
        self._retry_connrefused = retry_connrefused
        self._retry_dns_error = retry_dns_error
        self._redirects_remaining = max_redirects
        self._state = self.State.normal
        self._redirect_codes = REDIRECT_STATUS_CODES

        if robots_txt_pool:
            self._robots_txt_subsession = RobotsTxtSubsession(robots_txt_pool,
                self._new_request_instance)
        else:
            self._robots_txt_subsession = None

    def new_request(self, url_record, url_info):
        if not self._filter_test_url(url_info, url_record):
            _logger.debug('Rejecting {url} due to filters.'.format(
                url=url_info.url))
            return

        self._request = self._new_request_instance(
            url_info.url,
            url_record.referrer,
        )

        if self._file_writer:
            self._file_writer.rewrite_request(self._request)

        if self._redirect_url:
            self._request = self._new_request_instance(
                self._redirect_url,
                url_record.referrer,
            )

        self._check_robots_and_rewrite()

        return self._request

    def _new_request_instance(self, url, referer=None):
        request = self._request_factory(url)

        if 'Referer' not in request.fields and referer:
            request.fields['Referer'] = referer

        return request

    def _check_robots_and_rewrite(self):
        if not self._robots_txt_subsession:
            return

        if self._state == self.State.normal:
            try:
                ok = self._robots_txt_subsession.can_fetch(self._request)
            except RobotsTxtNotLoaded:
                self._state = self.State.robotstxt
                self._request = self._robots_txt_subsession.rewrite_request(
                    self._request)
            else:
                if not ok:
                    _logger.debug('Rejecting {url} due to robots.txt'.format(
                        url=self._request.url_info.url))
                    self._request = None
        else:
            self._request = self._robots_txt_subsession.rewrite_request(
                self._request)

    def accept_response(self, response, error=None):
        if self._state == self.State.robotstxt:
            return self._accept_robots_txt_response(response)

        if error:
            return self._accept_error(error)

        self._redirect_url = None

        if response.status_code in self._redirect_codes:
            return self._handle_redirect(response)

        elif response.status_code == 200:
            _logger.debug('Got a document.')
            self._scrape_document(self._request, response)
            if self._file_writer:
                self._file_writer.write_response(self._request, response)
            self._waiter.reset()
            self._statistics.increment(response.body.content_size)
            return Status.done

        elif response.status_code == 404:
            self._waiter.reset()
            return Status.skipped

        self._waiter.increment()

        self._statistics.errors[ServerError] += 1

        return Status.error

    def _accept_error(self, error):
        self._statistics.errors[type(error)] += 1
        self._waiter.increment()

        if isinstance(error, ConnectionRefused) \
        and not self._retry_connrefused:
            return Status.skipped
        if isinstance(error, DNSNotFound) and not self._retry_dns_error:
            return Status.skipped

        return Status.error

    def _accept_robots_txt_response(self, response):
        result = self._robots_txt_subsession.check_response(response)

        if result == RobotsTxtSubsession.Result.done:
            self._state = self.State.normal
        elif result == RobotsTxtSubsession.Result.fail:
            self._state = self.State.normal
        elif result == RobotsTxtSubsession.Result.redirect:
            pass
        elif result == RobotsTxtSubsession.Result.retry:
            self._waiter.increment()
        else:
            raise NotImplementedError()

    def _handle_redirect(self, response):
        if 'location' in response.fields and self._redirects_remaining > 0:
            url = response.fields['location']
            url = urllib.parse.urljoin(self._request.url_info.url, url)
            _logger.debug('Got redirect to {url}.'.format(url=url))
            self._redirect_url = url
            self._waiter.reset()
            self._redirects_remaining -= 1
            return
        else:
            _logger.warning(_('Redirection failure.'))
            self._statistics.errors[ProtocolError] += 1
            return Status.error

    def wait_time(self):
        return self._waiter.get()

    def get_inline_urls(self):
        return self._inline_urls

    def get_linked_urls(self):
        return self._linked_urls

    def _filter_test_url(self, url_info, url_record):
        results = []
        for url_filter in self._url_filters:
            result = url_filter.test(url_info, url_record)
            _logger.debug(
                'URL Filter test {0} returned {1}'.format(url_filter, result))
            results.append(result)

        return all(results)

    def _scrape_document(self, request, response):
        for scraper in self._document_scrapers:
            new_inline_urls, new_linked_urls = scraper.scrape(
                request, response) or ((), ())
            self._inline_urls.update(new_inline_urls)
            self._linked_urls.update(new_linked_urls)

        _logger.debug('Found URLs: inline={0} linked={1}'.format(
            len(self._inline_urls), len(self._linked_urls)
        ))


class RobotsTxtPool(object):
    def __init__(self):
        self._parsers = {}

    def has_parser(self, request):
        key = self.url_info_key(request.url_info)
        return key in self._parsers

    def can_fetch(self, request):
        key = self.url_info_key(request.url_info)

        parser = self._parsers[key]
        return parser.is_allowed(request.fields.get('user-agent', ''),
            request.url_info.url)

    def load_robots_txt(self, url_info, text):
        key = self.url_info_key(url_info)
        parser = robotexclusionrulesparser.RobotExclusionRulesParser()
        parser.parse(text)

        self._parsers[key] = parser

    @classmethod
    def url_info_key(cls, url_info):
        return (url_info.scheme, url_info.hostname, url_info.port)
