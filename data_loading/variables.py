mappingsElastic = {
    "mappings": {
        "runtime": {
            "DaysBeforePublishing": {
                "type": "long",
                "script": {
                    "source": "if (doc['MissedDate'].size()!=0) { emit((doc['PublishedDate'].value.millis - doc['MissedDate'].value.millis) / 1000 / 60 / 60 / 24) }"
                }
            },
            "MissedDays": {
                "type": "long",
                "script": {
                    "source": "if (doc['FoundDate'].size()!=0) { emit((doc['FoundDate'].value.millis - doc['PublishedDate'].value.millis) / 1000 / 60 / 60 / 24 + 1) }"
                }
            },
            "SignsNumber": {
                "type": "long",
                "script": {
                    "source": "emit(doc['Signs.characteristic.keyword'].length)"
                }
            }
        },
        "properties": {
            "Additional": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "Age": {
                "properties": {
                    "years": {
                        "type": "long"
                    }
                }
            },
            "FoundDate": {
                "type": "date"
            },
            "Gender": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "Location": {
                "properties": {
                    "value": {
                        "properties": {
                            "name": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            },
                            "number": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            },
                            "type": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "ShortLocation": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "LocationCoordinates": {
                "type": "geo_point",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "MissedDate": {
                "type": "date"
            },
            "MissedDays": {
                "type": "long"
            },
            "Name": {
                "properties": {
                    "first": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "last": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "middle": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    }
                }
            },
            "PublishedDate": {
                "type": "date"
            },
            "Signs": {
                "properties": {
                    "characteristic": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "values": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    }
                }
            },
            "StartDate": {
                "type": "date"
            },
            "Status": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "URL": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            }
        }
    }
}
