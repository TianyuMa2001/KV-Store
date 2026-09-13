import unittest
from unittest.mock import patch
from benchmark import validate_write, listening


class BenchmarkTests(unittest.TestCase):
    def test_insufficient_or_misconfigured_ack_is_rejected(self):
        for body in ({'acks':4,'required':5}, {'acks':5,'required':3}, {}):
            with self.assertRaises(AssertionError):
                validate_write(201,body,{'w':5,'fault':'none'})

    def test_unreachable_w_five_never_acknowledges(self):
        case={'w':5,'fault':'unreachable'}
        with self.assertRaises(AssertionError):
            validate_write(201,{'acks':5,'required':5},case)
        validate_write(503,{'localApplied':True},case)

    def test_successful_quorum_is_accepted(self):
        validate_write(201,{'acks':3,'required':3},{'w':3,'fault':'none'})

    def test_port_check_detects_listener(self):
        with patch('benchmark.socket.socket') as socket:
            socket.return_value.__enter__.return_value.connect_ex.return_value=0
            self.assertTrue(listening(12345))
            socket.return_value.__enter__.return_value.connect_ex.return_value=1
            self.assertFalse(listening(12345))


if __name__=='__main__':
    unittest.main()
